import boto3
import datetime
import os, json
from datetime import date

SNS_TOPIC_ARN = os.environ['SnsTopicArn']
ENDPOINT = os.environ['PhoneNumber']
EMR_BUCKET = os.environ['EMRBucket']
emrclient = boto3.client('emr')
eventclient = boto3.client('events')
snsclient = boto3.client('sns')

def createClusterNew(name):
    response = emrclient.run_job_flow(Name=name,
                                      LogUri=EMR_BUCKET,
                                      ReleaseLabel='emr-6.2.0',
                                      Instances={ 'InstanceGroups': [{'Market': 'ON_DEMAND',
                                                                      'InstanceRole': 'MASTER',
                                                                      'InstanceType': 'm4.4xlarge',
                                                                      'InstanceCount': 1}],
                                                  'KeepJobFlowAliveWhenNoSteps' : False,
                                                  'Ec2KeyName': 'practicaEMR',
                                                  'Ec2SubnetId': "subnet-25601b68"},
                                      Applications=[ {'Name': 'Spark'}, {'Name': 'Ganglia'}],
                                      JobFlowRole='EMR_EC2_DefaultRole',ServiceRole='EMR_DefaultRole',VisibleToAllUsers=True,
                                      Steps=[{'Name': 'PracticaEMR','ActionOnFailure': 'CONTINUE','HadoopJarStep': {'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar','Args': ['s3://misdatosemr/scripts/orchestador.sh']}}],
                                      Tags=[{ 'Key':'Environment', 'Value':'Dev'},{ 'Key':'Name', 'Value':'proceso EMR'},{ 'Key':'Application', 'Value':'EMR'}])

    return response


def createEventRule(name, cluster_id):
    """
    https://stackoverflow.com/questions/64869278/aws-cloudwatch-alarm-on-emr-using-boto3
    https://stackoverflow.com/questions/47148136/adding-in-cloudwatch-events-to-emr-cluster
    """
    ruleresponse = eventclient.put_rule(
        Name=name,
        EventPattern=f"""{{
            "source": ["aws.emr"],
            "detail-type": ["EMR Cluster State Change"],
            "detail": {{
                "clusterid": ["{cluster_id}"],
                "state": ["STARTING", "TERMINATED", "TERMINATED_WITH_ERRORS"]
            }}
        }}""",
        State="ENABLED"
    )

    response = eventclient.put_targets(
        Rule=name,
        Targets = [
            {
                'Id': "1",
                'Arn': SNS_TOPIC_ARN
            }])

    return response

def lambda_handler(event, context):
    print(f"Received parameters: \n{SNS_TOPIC_ARN}\n{ENDPOINT}")

    res = createClusterNew("PracticaEMR")
    clusterId = res['JobFlowId']
    createEventRule(f"PracticaEMRRegla", clusterId)
    snsres = snsclient.subscribe(
        TopicArn=SNS_TOPIC_ARN,
        Protocol='sms',
        Endpoint=ENDPOINT,
        ReturnSubscriptionArn=True
    )

    ## Agregar en otra Lambda y agregar a put_targets de la regla de cloudwatch
    # snsclient.publish(
    #     PhoneNumber=ENDPOINT,
    #     Message='Alerta de SNS enviado',
    #     Subject='practica SNS'
    # )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Done"
        }),
    }

