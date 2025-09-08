import os, sys, json, time, hashlib, argparse
from pathlib import Path
from typing import Any, Dict, List
import boto3, yaml
from botocore.exceptions import ClientError

SEP = "#"  # separator used in FQIDs written to DynamoDB keys

def _die(msg: str):
    print(f"[ERROR] {msg}", file=sys.stderr); sys.exit(1)

def _sha256_short(obj: Any) -> str:
    b = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(b).hexdigest()[:16]

def _load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _s3_put_json(s3, bucket: str, key: str, obj: Any, dry: bool):
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    if dry:
        print(f"[DRY] S3 PUT s3://{bucket}/{key} ({len(body)} bytes)"); return
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json",
                  CacheControl="max-age=0,no-cache,no-store")
    print(f"[OK ] S3 PUT s3://{bucket}/{key}")

def _ddb_put_live(table, item: Dict[str, Any], guard_version: bool, dry: bool):
    if dry:
        print(f"[DRY] DDB PUT {item['PK']} {item['SK']} -> {json.dumps(item, ensure_ascii=False)}"); return
    kwargs = {"Item": item}
    if guard_version and "version" in item:
        kwargs.update({
            "ConditionExpression": "attribute_not_exists(#v) OR #v <= :newv",
            "ExpressionAttributeNames": {"#v": "version"},
            "ExpressionAttributeValues": {":newv": item["version"]}
        })
    try:
        table.put_item(**kwargs)
        print(f"[OK ] DDB PUT {item['PK']} {item['SK']}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            print(f"[SKIP] Newer/equal version exists for {item['PK']} {item['SK']}")
        else:
            raise

def _discover_unit_yaml(unit_dir: Path) -> Path:
    in_dir = unit_dir / f"{unit_dir.name}.yaml"
    sibling = unit_dir.parent / f"{unit_dir.name}.yaml"
    if in_dir.exists(): return in_dir
    if sibling.exists(): return sibling
    _die(f"Unit YAML not found for {unit_dir.name}")

def _discover_episode_yaml(ep_dir: Path) -> Path:
    p = ep_dir / f"{ep_dir.name}.yaml"
    if p.exists(): return p
    _die(f"Episode YAML not found: {p}")

def _list_activity_yaml(ep_dir: Path) -> List[Path]:
    act_dir = ep_dir / "activities"
    if not act_dir.exists(): return []
    return sorted(p for p in act_dir.glob("a_*.yaml") if p.is_file())

def _build_manifest(unit_id: str, episode_id: str, a_yaml: Dict[str, Any]) -> Dict[str, Any]:
    aid = a_yaml.get("id")
    title = a_yaml.get("title", aid)
    version = int(a_yaml.get("version", 1))
    locale = a_yaml.get("locale", "en-US")
    questions = a_yaml.get("questions", [])
    return {
        "unitId": unit_id,
        "episodeId": episode_id,
        "activityId": aid,                         # local id
        "activityFqId": f"{unit_id}/{episode_id}/{aid}",  # for easy client logging
        "title": title,
        "version": version,
        "locale": locale,
        "total": len(questions),
        "questions": questions
    }

def _publish(root: Path, bucket: str, table_name: str, s3_prefix: str, region: str, dry: bool, profile: str):
    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3", region_name=region)
    ddb = session.resource("dynamodb", region_name=region)
    table = ddb.Table(table_name)

    if not root.exists(): _die(f"Root not found: {root}")

    for unit_dir in sorted(p for p in root.iterdir() if p.is_dir() and p.name.startswith("u_")):
        unit_yaml = _load_yaml(_discover_unit_yaml(unit_dir))
        unit_id = unit_yaml.get("id") or unit_dir.name
        unit_title = unit_yaml.get("title", unit_id)
        unit_content = unit_yaml.get("content", "")

        episodes_root = unit_dir / "episodes"
        local_ep_ids: List[str] = []
        fq_ep_ids: List[str] = []

        if episodes_root.exists():
            for ep_dir in sorted(p for p in episodes_root.iterdir() if p.is_dir() and p.name.startswith("e_")):
                local_ep_ids.append(ep_dir.name)
                fq_ep_ids.append(f"{unit_id}{SEP}{ep_dir.name}")

        # UNIT LIVE
        unit_item = {
            "PK": f"UNIT#{unit_id}",
            "SK": "LIVE",
            "entityType": "UNIT_LIVE",
            "title": unit_title,
            "content": unit_content,
            "episodeIds": local_ep_ids,         # local ids under this unit
            "episodeFqIds": fq_ep_ids,          # helpful globally
            "updatedAt": int(time.time())
        }
        _ddb_put_live(table, unit_item, guard_version=False, dry=dry)

        if not episodes_root.exists():
            print(f"[WARN] No episodes folder for unit {unit_id}")
            continue

        for ep_dir in sorted(p for p in episodes_root.iterdir() if p.is_dir() and p.name.startswith("e_")):
            ep_yaml = _load_yaml(_discover_episode_yaml(ep_dir))
            episode_id = ep_yaml.get("id") or ep_dir.name
            episode_title = ep_yaml.get("title", episode_id)

            acts = _list_activity_yaml(ep_dir)
            local_act_ids = [p.stem for p in acts]  # e.g., a_1
            fq_act_ids = [f"{unit_id}{SEP}{episode_id}{SEP}{p.stem}" for p in acts]

            # EPISODE LIVE (hierarchical PK)
            episode_item = {
                "PK": f"EPISODE#{unit_id}{SEP}{episode_id}",
                "SK": "LIVE",
                "entityType": "EPISODE_LIVE",
                "unitId": unit_id,
                "episodeId": episode_id,
                "title": episode_title,
                "activityIds": local_act_ids,
                "activityFqIds": fq_act_ids,
                "updatedAt": int(time.time())
            }
            _ddb_put_live(table, episode_item, guard_version=False, dry=dry)

            # Activities
            for a_path in acts:
                a_yaml = _load_yaml(a_path)
                activity_id = a_yaml.get("id") or a_path.stem
                activity_title = a_yaml.get("title", activity_id)
                version = int(a_yaml.get("version", 1))

                manifest = _build_manifest(unit_id, episode_id, a_yaml)
                mhash = _sha256_short(manifest)
                s3_key = f"{s3_prefix}/{unit_id}/{episode_id}/{activity_id}/v{manifest['version']}/manifest-{mhash}.json"
                _s3_put_json(s3, bucket, s3_key, manifest, dry)

                # ACTIVITY LIVE (hierarchical PK)
                activity_item = {
                    "PK": f"ACTIVITY#{unit_id}{SEP}{episode_id}{SEP}{activity_id}",
                    "SK": "LIVE",
                    "entityType": "ACTIVITY_LIVE",
                    "unitId": unit_id,
                    "episodeId": episode_id,
                    "activityId": activity_id,              # local
                    "activityFqid": f"{unit_id}{SEP}{episode_id}{SEP}{activity_id}",
                    "title": activity_title,
                    "locale": manifest.get("locale", "en-US"),
                    "manifestS3Key": f"s3://{bucket}/{s3_key}",
                    "totalQuestions": manifest["total"],
                    "version": version,
                    "updatedAt": int(time.time())
                }
                _ddb_put_live(table, activity_item, guard_version=True, dry=dry)

    print("[DONE] publish complete.")

def main():
    ap = argparse.ArgumentParser(description="Publish canonical Liwanag content (units, episodes, activities, questions) to DynamoDB & S3.")
    ap.add_argument("--root", default="../content/units", help="Root folder (default: content/units)")
    ap.add_argument("--prefix", default="activities", help="S3 key prefix (default: activities)")
    ap.add_argument("--dry-run", action="store_true", help="Print actions only")
    ap.add_argument("--profile", default="default", help="AWS CLI profile name (default: default)")
    args = ap.parse_args()

    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-1"
    table = os.environ.get("CONTENT_TABLE", "ContentTable")
    bucket = os.environ.get("CONTENT_BUCKET", "liwanag-content-bucket")

    if not region: _die("Set AWS_REGION")
    if not table:  _die("Set CONTENT_TABLE")
    if not bucket: _die("Set CONTENT_BUCKET")

    _publish(root=Path(args.root), bucket=bucket, table_name=table,
            s3_prefix=args.prefix.strip("/"), region=region, dry=args.dry_run, profile=args.profile)

if __name__ == "__main__":
    main()
