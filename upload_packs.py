#!/usr/bin/env python3
"""
Upload generated packs to S3-compatible storage.

Uploads:
- packs.json.gz to the bucket root with short cache (2 hours)
- {version}/*.json.gz files with long cache (1 year)
"""

import argparse
import sys
from pathlib import Path

import boto3
from botocore.config import Config

from utils import load_env_file, format_size

# Cache control headers
CACHE_INDEX = "public, max-age=7200"  # 2 hours for packs.json.gz
CACHE_PACKS = "public, max-age=31536000"  # 1 year for versioned packs


def upload_packs(
    packs_dir: Path,
    pack_version: str,
    s3_key_id: str,
    s3_secret: str,
    s3_bucket: str,
    s3_endpoint: str,
) -> bool:
    """
    Upload packs to S3.

    Args:
        packs_dir: Directory containing packs.json.gz and version subdirectory
        pack_version: Version string (e.g., "dec-2025")
        s3_key_id: S3 access key ID
        s3_secret: S3 secret access key
        s3_bucket: S3 bucket name
        s3_endpoint: S3 endpoint URL

    Returns:
        True if successful, False otherwise
    """
    # Validate paths exist
    index_file = packs_dir / "packs.json.gz"
    version_dir = packs_dir / pack_version

    if not index_file.exists():
        print(f"Error: Index file not found: {index_file}")
        return False

    if not version_dir.exists():
        print(f"Error: Version directory not found: {version_dir}")
        return False

    # Create S3 client
    s3 = boto3.client(
        's3',
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_key_id,
        aws_secret_access_key=s3_secret,
        config=Config(signature_version='s3v4'),
    )

    # Collect all files to upload
    pack_files = list(version_dir.glob("*.json.gz"))
    total_files = len(pack_files) + 1  # +1 for index
    total_size = index_file.stat().st_size + sum(f.stat().st_size for f in pack_files)

    print(f"\nUploading {total_files} files ({format_size(total_size)}) to {s3_bucket}")
    print(f"Endpoint: {s3_endpoint}")
    print()

    uploaded = 0
    failed = 0

    # Upload index file (packs.json.gz) to root
    try:
        print(f"  Uploading packs.json.gz ({format_size(index_file.stat().st_size)})...")
        s3.upload_file(
            str(index_file),
            s3_bucket,
            "packs.json.gz",
            ExtraArgs={
                'ContentType': 'application/json',
                'ContentEncoding': 'gzip',
                'CacheControl': CACHE_INDEX,
            }
        )
        uploaded += 1
    except Exception as e:
        print(f"    Failed: {e}")
        failed += 1

    # Upload versioned pack files
    for pack_file in pack_files:
        s3_key = f"{pack_version}/{pack_file.name}"
        try:
            size = pack_file.stat().st_size
            print(f"  Uploading {s3_key} ({format_size(size)})...")
            s3.upload_file(
                str(pack_file),
                s3_bucket,
                s3_key,
                ExtraArgs={
                    'ContentType': 'application/json',
                    'ContentEncoding': 'gzip',
                    'CacheControl': CACHE_PACKS,
                }
            )
            uploaded += 1
        except Exception as e:
            print(f"    Failed: {e}")
            failed += 1

    print()
    print(f"Uploaded: {uploaded}/{total_files}")
    if failed > 0:
        print(f"Failed: {failed}")
        return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Upload packs to S3-compatible storage"
    )
    parser.add_argument(
        "packs_dir",
        type=Path,
        help="Directory containing packs.json.gz and version subdirectory"
    )
    parser.add_argument(
        "pack_version",
        type=str,
        help="Pack version (e.g., dec-2025)"
    )
    args = parser.parse_args()

    # Load environment variables
    env_vars = load_env_file()

    # Get S3 credentials
    s3_key_id = env_vars.get("S3_KEY_ID")
    s3_secret = env_vars.get("S3_SECRET")
    s3_bucket = env_vars.get("S3_BUCKET")
    s3_endpoint = env_vars.get("S3_ENDPOINT")

    missing = []
    if not s3_key_id:
        missing.append("S3_KEY_ID")
    if not s3_secret:
        missing.append("S3_SECRET")
    if not s3_bucket:
        missing.append("S3_BUCKET")
    if not s3_endpoint:
        missing.append("S3_ENDPOINT")

    if missing:
        print(f"Error: Missing S3 credentials in .env: {', '.join(missing)}")
        sys.exit(1)

    print("=" * 50)
    print("  Upload Packs to S3")
    print("=" * 50)

    success = upload_packs(
        packs_dir=args.packs_dir,
        pack_version=args.pack_version,
        s3_key_id=s3_key_id,
        s3_secret=s3_secret,
        s3_bucket=s3_bucket,
        s3_endpoint=s3_endpoint,
    )

    if success:
        print("\nUpload complete!")
    else:
        print("\nUpload failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
