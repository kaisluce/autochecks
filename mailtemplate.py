import requests
import msal


def get_access_token(client_id, client_secret, tenant_id):
   authority = f"https://login.microsoftonline.com/{tenant_id}"
   app = msal.ConfidentialClientApplication(
       client_id,
       authority=authority,
       client_credential=client_secret
   )
   token_response = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
   return token_response.get("access_token")


def send_email(access_token, sender, recipient, subject, body, attachment_path=None):
   endpoint = "https://graph.microsoft.com/v1.0/users/{sender}/sendMail"
   headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
   # Email message structure
   email_message = {
       "Message": {
           "Subject": subject,
           "Body": {
               "ContentType": "Text",
               "Content": body
           },
           "ToRecipients": [
               {"EmailAddress": {"Address": recipient}}
           ]
       },
       "SaveToSentItems": "true"
   }
   # Add attachment if provided
   if attachment_path:
       with open(attachment_path, "rb") as file:
           content_bytes = file.read().encode("base64").decode("utf-8")
       email_message["Message"]["Attachments"] = [{
           "@odata.type": "#microsoft.graph.fileAttachment",
           "Name": attachment_path.split("/")[-1],
           "ContentBytes": content_bytes
       }]
   response = requests.post(endpoint.format(sender=sender), headers=headers, json=email_message)
   if response.status_code == 202:
       print("Email sent successfully!")
   else:
       print(f"Failed to send email: {response.json()}")
# Example Usage
if __name__ == "__main__":
   CLIENT_ID = "<your-client-id>"
   CLIENT_SECRET = "<your-client-secret>"
   TENANT_ID = "<your-tenant-id>"
   token = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID)
   send_email(
       access_token=token,
       sender="youremail@domain.com",
       recipient="recipient@domain.com",
       subject="Test Email",
       body="This is a test email sent using Microsoft Graph API.",
       attachment_path="path/to/attachment.txt"
   )