    def postTeams(self, webhook_url, metrics): 

        print('Building the payload to be sent to Microsoft Teams...')
      
        if metrics:
            for metric in metrics:

                message = '```'

                for key,value in metric.items():
                    message += f'{key}: {value}\n'                          
                
                status = metric.get('workflow_run_status',None)

                if status == 'SUCCEEDED':
                    status = 'Succeeded ✅'
                    color = '00FF00'
                else:
                    status = 'Failed ❌'
                    color = 'FF0000 '
                    message += f'\n⚠️ {workflow_name} workflow failed and requires attention from the Data Engineering team ⚠️'
                
                title = f'AWS ETL Notifications: {workflow_name} Workflow {status}'
            
                # Content for teams payload
                teams_payload = {
                    
                    '@type': 'MessageCard',
                    '@context': 'http://schema.org/extensions',
                    'themeColor': color,
                    'summary': 'Announcement Summary',
                    'title': title,
                    'sections': [{
                        'activityTitle': f'Service: AWS Glue',
                        'activitySubtitle': 'Summary from the last workflow execution when ingesting data from CDH to AWS:', 
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