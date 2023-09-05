import json
import urllib3
http = urllib3.PoolManager()

def postTeams(webhook_url, metrics): 

    """
    Sends a notification message to a Microsoft Teams channel using a webhook URL.

    Parameters:
    - webhook_url (str): The URL of the Microsoft Teams channel where the notification will be sent.
    - metrics (dict): A dictionary containing information and metrics about a workflow.

    Description:
    This function constructs a notification message with details about a specific workflow, including its name,
    status (success or failure), and additional metrics. It is designed for notifying a team about the status of
    AWS ETL workflows. The constructed message is sent to the specified Microsoft Teams channel using the provided webhook URL.

    If the workflow succeeds, a success message with a green checkmark is displayed. If it fails, a failure message
    with a red cross is displayed, and additional information is added to the message.

    The function handles the creation of the JSON payload, sets appropriate headers for the HTTP request, and
    communicates with the Microsoft Teams channel to send the notification.
    """

    print('Building the payload to be sent to Microsoft Teams...')
    
    if metrics:

        workflow_name = metrics.get('workflow_name', None)
        status = metrics.get('workflow_run_status',None)

        if status == 'SUCCEEDED':
            status = 'Succeeded ✅'
            color = '00FF00'
        else:
            status = 'Failed ❌'
            color = 'FF0000 '
            message += f'\n⚠️ {workflow_name} workflow failed and requires attention from the Data Engineering team ⚠️'

        title = f'AWS ETL Notifications: {workflow_name} Workflow {status}'
        message = '```'

        for key,value in metrics.items():
            message += f'{key}: {value}\n'                          
           
        # Content for teams payload
        teams_payload = {
            
            '@type': 'MessageCard',
            '@context': 'http://schema.org/extensions',
            'themeColor': color,
            'summary': 'Announcement Summary',
            'title': title,
            'sections': [{
                'activityTitle': f'AWS Service: Glue',
                'activitySubtitle': 'Summary from the last workflow execution when ingesting data to AWS:', 
                'text': message                 
            }]
        }
        
        # Convert teams payload to a json string  
        teams_payload_json = json.dumps(teams_payload)
        headers = {'Content-Type': 'application/json'}
        
        # Post request (aws to microsft teams)
        response = http.request('POST'
                                ,webhook_url
                                ,headers=headers
                                ,body=teams_payload_json)

        if response.status != 200:
            print(f'Request to Microsfot Teams returned an error {response.status}.. the response is:{response.data.decode("utf-8")}')
        else:
            print('Message sent successfully to Microsfot Teams!')
    
    else:
        print('No Message to be sent to Microsoft Teams!')
