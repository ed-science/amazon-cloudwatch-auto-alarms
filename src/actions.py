import boto3
import logging
from botocore.config import Config
from os import getenv
from datetime import datetime

logger = logging.getLogger()
log_level = getenv("LOGLEVEL", "INFO")
level = logging.getLevelName(log_level)
logger.setLevel(level)
valid_comparators = ['GreaterThanOrEqualToThreshold', 'GreaterThanThreshold', 'LessThanThreshold',
                     'LessThanOrEqualToThreshold']


def boto3_client(resource, assumed_credentials=None):
    config = Config(
        retries=dict(
            max_attempts=40
        )
    )
    return (
        boto3.client(
            resource,
            aws_access_key_id=assumed_credentials['AccessKeyId'],
            aws_secret_access_key=assumed_credentials['SecretAccessKey'],
            aws_session_token=assumed_credentials['SessionToken'],
            config=config,
        )
        if assumed_credentials
        else boto3.client(resource, config=config)
    )


def check_alarm_tag(instance_id, tag_key):
    try:
        ec2_client = boto3_client('ec2')
        # does instance have appropriate alarm tag?
        instance = ec2_client.describe_instances(
            Filters=[
                {
                    'Name': 'tag-key',
                    'Values': [
                        tag_key
                    ]
                }
            ],
            InstanceIds=[
                instance_id
            ]

        )
        # can only be one instance when called by CloudWatch Events
        if 'Reservations' in instance and len(instance['Reservations']) > 0 and len(
                instance['Reservations'][0]['Instances']) > 0:
            ec2_client.create_tags(
                Resources=[
                    instance_id
                ],
                Tags=[
                    {
                        'Key': tag_key,
                        'Value': str(datetime.utcnow())
                    }
                ]
            )
            return instance['Reservations'][0]['Instances'][0]
        else:
            return False

    except Exception as e:
        # If any other exceptions which we didn't expect are raised
        # then fail and log the exception message.
        logger.error(
            f'Failure describing instance {instance_id} with tag key: {tag_key} : {e}'
        )

        raise


def process_lambda_alarms(function_name, tags, activation_tag, default_alarms, sns_topic_arn, alarm_separator):
    activation_tag = tags.get(activation_tag, 'not_found')
    if activation_tag == 'not_found':
        logger.debug(f'Activation tag not found for {function_name}, nothing to do')
        return True
    else:
        logger.debug(f'Processing function specific alarms for: {default_alarms}')
        for tag_key in tags:
            if tag_key.startswith('AutoAlarm'):
                default_alarms['AWS/Lambda'].append({'Key': tag_key, 'Value': tags[tag_key]})

        # get the default dimensions for AWS/EC2
        dimensions = [{'Name': 'FunctionName', 'Value': function_name}]
        for tag in default_alarms['AWS/Lambda']:
            alarm_properties = tag['Key'].split(alarm_separator)
            Namespace = alarm_properties[1]
            MetricName = alarm_properties[2]
            ComparisonOperator = alarm_properties[3]
            Period = alarm_properties[4]
            Statistic = alarm_properties[5]

            AlarmName = f'AutoAlarm-{function_name}-{Namespace}-{MetricName}-{ComparisonOperator}-{Period}-{Statistic}'

            create_alarm(AlarmName, MetricName, ComparisonOperator, Period, tag['Value'], Statistic, Namespace,
                         dimensions, sns_topic_arn)


def create_alarm_from_tag(id, alarm_tag, instance_info, metric_dimensions_map, sns_topic_arn, alarm_separator):
    alarm_properties = alarm_tag['Key'].split(alarm_separator)
    namespace = alarm_properties[1]
    MetricName = alarm_properties[2]
    dimensions = []
    for dimension_name in metric_dimensions_map.get(namespace, list()):
        dimension = {}

        if dimension_name == 'AutoScalingGroupName':
            if instance_asg := next(
                (
                    tag['Value']
                    for tag in instance_info['Tags']
                    if tag['Key'] == 'aws:autoscaling:groupName'
                ),
                None,
            ):
                dimension_value = instance_asg
                dimension['Name'] = dimension_name
                dimension['Value'] = dimension_value
                dimensions.append(dimension)
        elif dimension_value := instance_info.get(dimension_name, None):
            dimension['Name'] = dimension_name
            dimension['Value'] = dimension_value
            dimensions.append(dimension)
        else:
            logger.warning(
                f"Dimension {dimension_name} has been specified in APPEND_DIMENSIONS but  no dimension value exists, skipping..."
            )


    logger.debug(f"dimensions are {dimensions}")

    additional_dimensions = []

    if prop_end_index := next(
        (
            index
            for index, prop in enumerate(alarm_properties[3:], start=3)
            if prop in valid_comparators
        ),
        None,
    ):
        additional_dimensions.extend(alarm_properties[3:prop_end_index])
    else:
        logger.error(f'Unable to determine the dimensions for alarm tag: {alarm_tag}')
        raise Exception

    AlarmName = f'AutoAlarm-{id}-{namespace}-{MetricName}'
    properties_offset = 0
    if additional_dimensions:
        for num, dim in enumerate(additional_dimensions[::2]):
            val = additional_dimensions[num * 2 + 1]
            dimensions.append(
                {
                    'Name': dim,
                    'Value': val
                }
            )
            AlarmName = AlarmName + f'-{dim}-{val}'
            properties_offset = properties_offset + 2

    ComparisonOperator = alarm_properties[(properties_offset + 3)]
    Period = alarm_properties[(properties_offset + 4)]
    Statistic = alarm_properties[(properties_offset + 5)]

    AlarmName = AlarmName + f'-{ComparisonOperator}-{Period}-{Statistic}'

    create_alarm(AlarmName, MetricName, ComparisonOperator, Period, alarm_tag['Value'], Statistic, namespace,
                 dimensions, sns_topic_arn)


def process_alarm_tags(instance_id, instance_info, default_alarms, metric_dimensions_map, sns_topic_arn, cw_namespace,
                       create_default_alarms_flag, alarm_separator):
    tags = instance_info['Tags']

    ImageId = instance_info['ImageId']
    logger.info(f'ImageId is: {ImageId}')
    platform = determine_platform(ImageId)

    logger.info(f'Platform is: {platform}')
    custom_alarms = {}
    # get all alarm tags from instance and add them into a custom tag list
    for instance_tag in tags:
        if instance_tag['Key'].startswith('AutoAlarm'):
            create_alarm_from_tag(instance_id, instance_tag, instance_info, metric_dimensions_map, sns_topic_arn, alarm_separator)

    if create_default_alarms_flag == 'true':
        for alarm_tag in default_alarms['AWS/EC2']:
            create_alarm_from_tag(instance_id, alarm_tag, instance_info, metric_dimensions_map, sns_topic_arn, alarm_separator)

        for alarm_tag in default_alarms[cw_namespace][platform]:
            create_alarm_from_tag(instance_id, alarm_tag, instance_info, metric_dimensions_map, sns_topic_arn, alarm_separator)
    else:
        logger.info("Default alarm creation is turned off")


def determine_platform(imageid):
    try:
        ec2_client = boto3_client('ec2')
        # does instance have appropriate alarm tag?
        image_info = ec2_client.describe_images(
            ImageIds=[
                imageid
            ]

        )

        if 'Images' not in image_info or len(image_info['Images']) <= 0:
            return None

        platform_details = image_info['Images'][0]['PlatformDetails']
        logger.debug(f'Platform details of image: {platform_details}')
        if 'Windows' in platform_details or 'SQL Server' in platform_details:
            return 'Windows'
        elif 'Red Hat' in platform_details:
            return 'Red Hat'
        elif 'SUSE' in platform_details:
            return 'SUSE'
        elif 'Linux/UNIX' in platform_details:
            return (
                'Ubuntu'
                if 'ubuntu' in image_info['Images'][0]['Description'].lower()
                or 'ubuntu' in image_info['Images'][0]['Name'].lower()
                else 'Amazon Linux'
            )

        else:
            return None
    except Exception as e:
        # If any other exceptions which we didn't expect are raised
        # then fail and log the exception message.
        logger.error(f'Failure describing image {imageid}: {e}')
        raise


def convert_to_seconds(s):
    try:
        seconds_per_unit = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
        return int(s[:-1]) * seconds_per_unit[s[-1]]
    except Exception as e:
        # If any other exceptions which we didn't expect are raised
        # then fail and log the exception message.
        logger.error(f'Error converting threshold string {s} to seconds!')
        raise


# Alarm Name Format: AutoAlarm-<InstanceId>-<Statistic>-<MetricName>-<ComparisonOperator>-<Threshold>-<Period>
# Example:  AutoAlarm-i-00e4f327736cb077f-CPUUtilization-GreaterThanThreshold-80-5m
def create_alarm(AlarmName, MetricName, ComparisonOperator, Period, Threshold, Statistic, Namespace, Dimensions,
                 sns_topic_arn):
    AlarmDescription = 'Alarm created by lambda function CloudWatchAutoAlarms'

    try:
        Period = convert_to_seconds(Period)
    except Exception as e:
        # If any other exceptions which we didn't expect are raised
        # then fail and log the exception message.
        logger.error(
            f'Error converting Period specified {Period} to seconds for Alarm {AlarmName}!: {e}'
        )


    Threshold = float(Threshold)
    try:
        cw_client = boto3_client('cloudwatch')

        alarm = {
            'AlarmName': AlarmName,
            'AlarmDescription': AlarmDescription,
            'MetricName': MetricName,
            'Namespace': Namespace,
            'Dimensions': Dimensions,
            'Period': Period,
            'EvaluationPeriods': 1,
            'Threshold': Threshold,
            'ComparisonOperator': ComparisonOperator,
            'Statistic': Statistic
        }

        if sns_topic_arn is not None:
            alarm['AlarmActions'] = [sns_topic_arn]

        cw_client.put_metric_alarm(**alarm)

        logger.info(f'Created alarm {AlarmName}')

    except Exception as e:
        # If any other exceptions which we didn't expect are raised
        # then fail and log the exception message.
        logger.error(f'Error creating alarm {AlarmName}!: {e}')


def delete_alarms(name):
    try:
        AlarmNamePrefix = f"AutoAlarm-{name}"
        cw_client = boto3_client('cloudwatch')
        logger.info(f'calling describe alarms with prefix {AlarmNamePrefix}')
        response = cw_client.describe_alarms(
            AlarmNamePrefix=AlarmNamePrefix,
        )
        alarm_list = []
        logger.info(f'Response from describe_alarms(): {response}')
        if 'MetricAlarms' in response:
            for alarm in response['MetricAlarms']:
                alarm_name = alarm['AlarmName']
                alarm_list.append(alarm_name)
        logger.info(f'deleting {alarm_list} for {name}')
        response = cw_client.delete_alarms(
            AlarmNames=alarm_list
        )
        return True
    except Exception as e:
        # If any other exceptions which we didn't expect are raised
        # then fail and log the exception message.
        logger.error(f'Error deleting alarms for {name}!: {e}')

def scan_and_process_alarm_tags(create_alarm_tag, default_alarms, metric_dimensions_map, sns_topic_arn,
                                   cw_namespace, create_default_alarms_flag, alarm_separator):
    try:
        ec2_client = boto3_client('ec2')
        for reservation in ec2_client.describe_instances()["Reservations"]:
            for instance in reservation["Instances"]:
                # Look for running instances only
                if instance["State"]["Code"]>16:
                    continue
                if check_alarm_tag(instance["InstanceId"], create_alarm_tag):
                    process_alarm_tags(instance["InstanceId"], instance, default_alarms, metric_dimensions_map,
                     sns_topic_arn, cw_namespace, create_default_alarms_flag, alarm_separator)

    except Exception as e:
        # If any other exceptions which we didn't expect are raised
        # then fail and log the exception message.
        logger.error(f'Failure describing reservations : {e}')
        raise