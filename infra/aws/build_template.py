# ruff: noqa: E501  (CloudFormation ARNs and Fn::Sub strings are long by nature)
"""Generate infra/aws/stack.yml, the CloudFormation template for the S3 data lake.

Run after changing a clean table's schema:

    python infra/aws/build_template.py          # rewrite stack.yml
    python infra/aws/build_template.py --check  # exit 1 if stack.yml is out of date (CI)

Athena column lists are read from the Spark job itself (pipeline.transform.spark_job.
build_tables) and written twice: into the Glue tables here, and into pipeline/lake_schema.json,
which pipeline/lake.py uses to write the Parquet files. One source, so Spark, the files and
Glue can never disagree.

What the stack creates (all in one region, everything tagged project=game-data-pipeline):
* S3 bucket for the lake: encrypted, public access blocked, TLS-only, Athena results
  auto-deleted after 7 days. The pipeline writes ~7 small Parquet files a day (ADR 0005).
* Glue database + one table per lake table, using partition projection on snapshot_date
  (no crawlers, no MSCK REPAIR: new days are queryable the moment they land).
* Athena workgroup with an enforced results location and a per-query scan limit.
* IAM role GitHub Actions assumes through OIDC: short-lived credentials, no stored keys,
  trusted only for this repository's main branch, with least-privilege permissions.
* A monthly cost budget with email alerts.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

OUT = Path(__file__).with_name("stack.yml")
SCHEMA_OUT = ROOT / "pipeline" / "lake_schema.json"
TAGS = [{"Key": "project", "Value": "game-data-pipeline"}]

DESCRIPTIONS = {
    "player_count_snapshots": "Live Steam player count per tracked game per fetch (hourly).",
    "player_counts_daily": "Min / max / average players per tracked game per day.",
    "app_prices": "Store price per tracked game per day, in cents (CAD).",
    "review_summary": "Steam review totals and positive ratio per game per day.",
    "review_themes": "Theme mention counts from 100 recent reviews per game per day (no review text).",
    "twitch_viewer_snapshots": "Twitch viewers and streams per game per fetch.",
    "market_snapshots": "Steam top-100 most played: rank and live players per game per fetch.",
}


def spark_to_hive(dtype) -> str:
    from pyspark.sql import types as T

    simple = {
        T.IntegerType: "int",
        T.LongType: "bigint",
        T.ShortType: "smallint",
        T.ByteType: "tinyint",
        T.StringType: "string",
        T.BooleanType: "boolean",
        T.DoubleType: "double",
        T.FloatType: "float",
        T.DateType: "date",
        T.TimestampType: "timestamp",
    }
    for cls, hive in simple.items():
        if isinstance(dtype, cls):
            return hive
    if isinstance(dtype, T.DecimalType):
        return f"decimal({dtype.precision},{dtype.scale})"
    raise TypeError(f"No Hive type for Spark type {dtype!r}")


def lake_schemas() -> dict[str, list[tuple[str, str]]]:
    """Column (name, Hive type) per lake table, excluding the snapshot_date partition column."""
    from pipeline.lake import LAKE_TABLES
    from pipeline.transform.spark_job import build_tables, get_spark

    spark = get_spark()
    try:
        with tempfile.TemporaryDirectory() as empty:
            tables = build_tables(spark, Path(empty), date(2026, 1, 1))
            return {
                name: [
                    (f.name, spark_to_hive(f.dataType)) for f in tables[name].schema.fields if f.name != "snapshot_date"
                ]
                for name in LAKE_TABLES
            }
    finally:
        spark.stop()


def glue_table(name: str, columns: list[tuple[str, str]]) -> dict:
    location = {"Fn::Sub": f"s3://${{LakeBucket}}/${{LakePrefix}}/clean/{name}/"}
    return {
        "Type": "AWS::Glue::Table",
        "Properties": {
            "CatalogId": {"Ref": "AWS::AccountId"},
            "DatabaseName": {"Ref": "GlueDatabase"},
            "TableInput": {
                "Name": name,
                "Description": DESCRIPTIONS.get(name, ""),
                "TableType": "EXTERNAL_TABLE",
                "PartitionKeys": [{"Name": "snapshot_date", "Type": "date"}],
                "Parameters": {
                    "classification": "parquet",
                    "projection.enabled": "true",
                    "projection.snapshot_date.type": "date",
                    "projection.snapshot_date.format": "yyyy-MM-dd",
                    "projection.snapshot_date.range": "2026-11-01,NOW",
                    "projection.snapshot_date.interval": "1",
                    "projection.snapshot_date.interval.unit": "DAYS",
                    "storage.location.template": {
                        "Fn::Sub": f"s3://${{LakeBucket}}/${{LakePrefix}}/clean/{name}/snapshot_date=${{!snapshot_date}}/"
                    },
                },
                "StorageDescriptor": {
                    "Columns": [{"Name": c, "Type": t} for c, t in columns],
                    "Location": location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    },
                },
            },
        },
    }


NAMED_QUERIES = {
    "take-two-share-by-day": (
        "Take-Two share of players across tracked games, per day",
        """-- Joins the lake to nothing: publisher groups are inlined so the query runs on S3 alone.
WITH groups(app_id, publisher_group) AS (
    VALUES (3240220, 'Take-Two'), (271590, 'Take-Two'), (1174180, 'Take-Two'), (3472040, 'Take-Two'),
           (1285190, 'Take-Two'), (289070, 'Take-Two'), (1295660, 'Take-Two')
)
SELECT d.snapshot_date,
       ROUND(100.0 * SUM(CASE WHEN g.publisher_group = 'Take-Two' THEN d.avg_players END) / SUM(d.avg_players), 1)
           AS take_two_share_pct
FROM player_counts_daily d
LEFT JOIN groups g ON g.app_id = d.app_id
GROUP BY d.snapshot_date
ORDER BY d.snapshot_date;""",
    ),
    "steam-top10-latest": (
        "Steam top 10 in the latest market snapshot",
        """SELECT rank, app_id, current_players, peak_in_game, captured_at
FROM market_snapshots
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM market_snapshots)
  AND captured_at = (SELECT MAX(captured_at) FROM market_snapshots
                     WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM market_snapshots))
ORDER BY rank
LIMIT 10;""",
    ),
    "review-themes-latest": (
        "Most-mentioned review themes per game, latest day",
        """SELECT app_id, theme, mentions, reviews_sampled,
       ROUND(100.0 * negative_mentions / NULLIF(mentions, 0), 0) AS negative_share_pct
FROM review_themes
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM review_themes)
  AND mentions > 0
ORDER BY app_id, mentions DESC;""",
    ),
}


def build(schemas: dict[str, list[tuple[str, str]]]) -> dict:
    bucket_arn = {"Fn::GetAtt": ["LakeBucket", "Arn"]}
    resources: dict = {
        "LakeBucket": {
            "Type": "AWS::S3::Bucket",
            "DeletionPolicy": "Retain",  # deleting the stack never deletes the data
            "UpdateReplacePolicy": "Retain",
            "Properties": {
                "BucketName": {"Fn::Sub": "gdp-lake-${AWS::AccountId}-${AWS::Region}"},
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [
                        {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}, "BucketKeyEnabled": True}
                    ]
                },
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
                "OwnershipControls": {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
                "LifecycleConfiguration": {
                    "Rules": [
                        {
                            "Id": "expire-athena-results",
                            "Status": "Enabled",
                            "Prefix": "athena-results/",
                            "ExpirationInDays": 7,
                        },
                        {
                            "Id": "abort-incomplete-uploads",
                            "Status": "Enabled",
                            "Prefix": "",
                            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
                        },
                    ]
                },
                "Tags": TAGS,
            },
        },
        "LakeBucketPolicy": {
            "Type": "AWS::S3::BucketPolicy",
            "Properties": {
                "Bucket": {"Ref": "LakeBucket"},
                "PolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "DenyInsecureTransport",
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "s3:*",
                            "Resource": [bucket_arn, {"Fn::Sub": "${LakeBucket.Arn}/*"}],
                            "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                        }
                    ],
                },
            },
        },
        "GlueDatabase": {
            "Type": "AWS::Glue::Database",
            "Properties": {
                "CatalogId": {"Ref": "AWS::AccountId"},
                "DatabaseInput": {
                    "Name": {"Ref": "DatabaseName"},
                    "Description": "Game data pipeline: Steam and Twitch facts (github.com/amyvwhite/game-data-pipeline)",
                },
            },
        },
        "AthenaWorkGroup": {
            "Type": "AWS::Athena::WorkGroup",
            "Properties": {
                "Name": {"Ref": "WorkGroupName"},
                "Description": "Queries over the game data lake",
                "RecursiveDeleteOption": True,
                "WorkGroupConfiguration": {
                    "EnforceWorkGroupConfiguration": True,
                    "PublishCloudWatchMetricsEnabled": True,
                    "BytesScannedCutoffPerQuery": 1073741824,  # 1 GB cap per query: a runaway query costs < $0.01
                    "EngineVersion": {"SelectedEngineVersion": "AUTO"},
                    "ResultConfiguration": {
                        "OutputLocation": {"Fn::Sub": "s3://${LakeBucket}/athena-results/"},
                        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                    },
                },
                "Tags": TAGS,
            },
        },
        "GitHubOIDCProvider": {
            "Type": "AWS::IAM::OIDCProvider",
            "Condition": "CreateOIDCProvider",
            "Properties": {
                "Url": "https://token.actions.githubusercontent.com",
                "ClientIdList": ["sts.amazonaws.com"],
                "ThumbprintList": [
                    "6938fd4d98bab03faadb97b34396831e3780aea1",
                    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
                ],
                "Tags": TAGS,
            },
        },
        "PipelineRole": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "RoleName": {"Fn::Sub": "gdp-github-actions-${AWS::Region}"},
                "Description": "Assumed by GitHub Actions (OIDC) to publish the game data lake",
                "MaxSessionDuration": 3600,
                "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Federated": {
                                    "Fn::If": [
                                        "CreateOIDCProvider",
                                        {"Ref": "GitHubOIDCProvider"},
                                        {
                                            "Fn::Sub": "arn:${AWS::Partition}:iam::${AWS::AccountId}:oidc-provider/token.actions.githubusercontent.com"
                                        },
                                    ]
                                }
                            },
                            "Action": "sts:AssumeRoleWithWebIdentity",
                            "Condition": {
                                "StringEquals": {
                                    "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                                    "token.actions.githubusercontent.com:sub": {
                                        "Fn::Sub": "${GitHubSubject}:ref:refs/heads/main"
                                    },
                                }
                            },
                        }
                    ],
                },
                "Policies": [
                    {
                        "PolicyName": "publish-lake",
                        "PolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Sid": "ListBucket",
                                    "Effect": "Allow",
                                    "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                                    "Resource": bucket_arn,
                                },
                                {
                                    "Sid": "LakeObjects",
                                    "Effect": "Allow",
                                    "Action": ["s3:GetObject", "s3:PutObject"],
                                    "Resource": [
                                        {"Fn::Sub": "${LakeBucket.Arn}/${LakePrefix}/*"},
                                        {"Fn::Sub": "${LakeBucket.Arn}/athena-results/*"},
                                    ],
                                },
                                {
                                    "Sid": "RunAthena",
                                    "Effect": "Allow",
                                    "Action": [
                                        "athena:StartQueryExecution",
                                        "athena:GetQueryExecution",
                                        "athena:GetQueryResults",
                                    ],
                                    "Resource": {
                                        "Fn::Sub": "arn:${AWS::Partition}:athena:${AWS::Region}:${AWS::AccountId}:workgroup/${WorkGroupName}"
                                    },
                                },
                                {
                                    "Sid": "ReadCatalog",
                                    "Effect": "Allow",
                                    "Action": [
                                        "glue:GetDatabase",
                                        "glue:GetTable",
                                        "glue:GetTables",
                                        "glue:GetPartitions",
                                    ],
                                    "Resource": [
                                        {
                                            "Fn::Sub": "arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:catalog"
                                        },
                                        {
                                            "Fn::Sub": "arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:database/${DatabaseName}"
                                        },
                                        {
                                            "Fn::Sub": "arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:table/${DatabaseName}/*"
                                        },
                                    ],
                                },
                            ],
                        },
                    }
                ],
                "Tags": TAGS,
            },
        },
        "MonthlyBudget": {
            "Type": "AWS::Budgets::Budget",
            "Properties": {
                "Budget": {
                    "BudgetName": "game-data-pipeline-monthly",
                    "BudgetType": "COST",
                    "TimeUnit": "MONTHLY",
                    "BudgetLimit": {"Amount": {"Ref": "MonthlyBudgetUSD"}, "Unit": "USD"},
                },
                "NotificationsWithSubscribers": [
                    {
                        "Notification": {
                            "NotificationType": nt,
                            "ComparisonOperator": "GREATER_THAN",
                            "Threshold": th,
                            "ThresholdType": "PERCENTAGE",
                        },
                        "Subscribers": [{"SubscriptionType": "EMAIL", "Address": {"Ref": "BudgetEmail"}}],
                    }
                    for nt, th in (("ACTUAL", 50), ("ACTUAL", 100), ("FORECASTED", 100))
                ],
            },
        },
    }
    for name, columns in schemas.items():
        resources["Table" + "".join(p.title() for p in name.split("_"))] = glue_table(name, columns)
    for key, (desc, sql) in NAMED_QUERIES.items():
        resources["Query" + "".join(p.title() for p in key.split("-"))] = {
            "Type": "AWS::Athena::NamedQuery",
            "Properties": {
                "Name": key,
                "Description": desc,
                "Database": {"Ref": "DatabaseName"},
                "WorkGroup": {"Ref": "AthenaWorkGroup"},
                "QueryString": sql,
            },
        }

    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Game data pipeline - S3 data lake, Glue/Athena catalog, GitHub OIDC role, cost budget. "
        "GENERATED by infra/aws/build_template.py - edit that, not this file.",
        "Parameters": {
            "GitHubSubject": {
                "Type": "String",
                "Default": "repo:amyvwhite@169624580/game-data-pipeline@1463284796",
                "AllowedPattern": "^repo:[^:]+$",
                "Description": "OIDC subject prefix of the one repo allowed to assume the role (main branch only). "
                "GitHub's immutable form repo:<owner>@<owner_id>/<repo>@<repo_id> survives renames, so a "
                "re-created repo with the same name cannot take over the role.",
            },
            "BudgetEmail": {
                "Type": "String",
                "Description": "Where cost alerts are emailed",
                "AllowedPattern": "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$",
            },
            "MonthlyBudgetUSD": {
                "Type": "Number",
                "Default": 1,
                "MinValue": 1,
                "Description": "Alert at 50% and 100% of this monthly spend (USD)",
            },
            "CreateOIDCProvider": {
                "Type": "String",
                "Default": "true",
                "AllowedValues": ["true", "false"],
                "Description": "false if this AWS account already trusts GitHub's OIDC provider",
            },
            "LakePrefix": {"Type": "String", "Default": "game-data-pipeline", "AllowedPattern": "^[a-z0-9-]+$"},
            "DatabaseName": {"Type": "String", "Default": "game_data", "AllowedPattern": "^[a-z0-9_]+$"},
            "WorkGroupName": {"Type": "String", "Default": "game-data"},
        },
        "Conditions": {"CreateOIDCProvider": {"Fn::Equals": [{"Ref": "CreateOIDCProvider"}, "true"]}},
        "Resources": resources,
        "Outputs": {
            "RoleArn": {
                "Description": "Set as the AWS_ROLE_ARN repository variable",
                "Value": {"Fn::GetAtt": ["PipelineRole", "Arn"]},
            },
            "BucketName": {"Description": "Set as the S3_BUCKET repository variable", "Value": {"Ref": "LakeBucket"}},
            "WorkGroup": {"Value": {"Ref": "AthenaWorkGroup"}},
            "Database": {"Value": {"Ref": "DatabaseName"}},
        },
    }


def render(schemas: dict[str, list[tuple[str, str]]]) -> str:
    class Dumper(yaml.SafeDumper):
        def ignore_aliases(self, data):  # CloudFormation rejects YAML anchors/aliases
            return True

    def str_rep(dumper, data):  # keep multi-line SQL readable
        style = "|" if "\n" in data else None
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)

    Dumper.add_representer(str, str_rep)
    body = yaml.dump(build(schemas), Dumper=Dumper, sort_keys=False, width=120, allow_unicode=True)
    return "# GENERATED by infra/aws/build_template.py - do not edit by hand.\n" + body


def render_schema(schemas: dict[str, list[tuple[str, str]]]) -> str:
    doc = {
        "_comment": "GENERATED by infra/aws/build_template.py from the Spark schemas - do not edit by hand.",
        "tables": {t: [{"name": c, "type": ty} for c, ty in cols] for t, cols in schemas.items()},
    }
    return json.dumps(doc, indent=2) + "\n"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--check", action="store_true", help="fail if the generated files don't match the Spark schemas")
    args = p.parse_args()
    schemas = lake_schemas()
    outputs = {OUT: render(schemas), SCHEMA_OUT: render_schema(schemas)}
    if args.check:
        stale = [p for p, text in outputs.items() if not p.exists() or p.read_text(encoding="utf-8") != text]
        for path in stale:
            print(f"{path.relative_to(ROOT)} is out of date: run python infra/aws/build_template.py", file=sys.stderr)
        if not stale:
            print("stack.yml and lake_schema.json match the Spark schemas")
        return 1 if stale else 0
    for path, text in outputs.items():
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
