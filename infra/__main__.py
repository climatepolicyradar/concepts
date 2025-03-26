import pulumi
import pulumi_aws as aws

# This potentially shoudl either be somewhere global - or specific for this apprunner instance
github_connection = aws.apprunner.Connection(
    "github_connection",
    connection_name="GitHubConnector",
    provider_type="GITHUB",
    opts=pulumi.ResourceOptions(protect=True),
)

apprunner_concepts_api_instance_role = aws.iam.Role(
    "apprunner-concepts-api-instance-role",
    assume_role_policy={
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": "sts:AssumeRole",
                "Effect": "Allow",
                "Principal": {"Service": "tasks.apprunner.amazonaws.com"},
                "Sid": "",
            }
        ],
    },
    description="Allows the concepts API to access S3 for the concepts stored there",
    inline_policies=[
        {
            "name": "apprunner-concepts-api-instance-policy",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": ["s3:GetObject", "s3:ListBucket"],
                        "Effect": "Allow",
                        "Resource": [
                            "arn:aws:s3:::cpr-production-document-cache",
                            "arn:aws:s3:::cpr-production-document-cache/*",
                        ],
                        "Sid": "VisualEditor0",
                    }
                ],
            },
        }
    ],
    name="apprunner-concepts-api-instance-role",
    opts=pulumi.ResourceOptions(protect=True),
)

concepts_api = aws.apprunner.Service(
    "concepts-api",
    auto_scaling_configuration_arn="arn:aws:apprunner:eu-west-1:532586131621:autoscalingconfiguration/DefaultConfiguration/1/00000000000000000000000000000001",
    health_check_configuration={
        "interval": 10,
        "protocol": "TCP",
        "timeout": 5,
    },
    instance_configuration={
        "instance_role_arn": apprunner_concepts_api_instance_role.arn,
    },
    network_configuration={
        "egress_configuration": {
            "egress_type": "DEFAULT",
        },
        "ingress_configuration": {
            "is_publicly_accessible": True,
        },
        "ip_address_type": "IPV4",
    },
    observability_configuration={
        "observability_enabled": False,
    },
    service_name="concepts-api",
    source_configuration={
        "authentication_configuration": {
            "connection_arn": github_connection.arn,
        },
        "code_repository": {
            "code_configuration": {
                # This isn't true as we use the apprunner.yaml from the repo
                # but I think we need it
                # TODO: validate this
                "code_configuration_values": {
                    "build_command": "pip install -r requirements.txt",
                    "runtime": "PYTHON_3",
                    "start_command": "python server.py",
                },
                "configuration_source": "REPOSITORY",
            },
            "repository_url": "https://github.com/climatepolicyradar/concepts",
            "source_code_version": {
                "type": "BRANCH",
                "value": "main",
            },
            "source_directory": "/",
        },
    },
    opts=pulumi.ResourceOptions(protect=True),
)
