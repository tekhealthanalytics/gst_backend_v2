from sib_api_v3_sdk.rest import ApiException
import sib_api_v3_sdk
from django.conf import settings
import base64
import os

configuration = sib_api_v3_sdk.Configuration()

# configuration.api_key['api-key'] = settings.BREVO_API_KEY
configuration.api_key['api-key'] = os.getenv("SENDINBLUE_KEY")
api_instance = sib_api_v3_sdk.TransactionalEmailsApi(
        sib_api_v3_sdk.ApiClient(configuration)
    )

def sendMail(subject,to,html_content, attachments=None):
    try:
        brevo_attachments = []
        if attachments:
            for file in attachments:
                filename = file.name
                file.seek(0)
                file_bytes = file.read()
                encoded = base64.b64encode(file_bytes).decode()
                brevo_attachments.append({
                    "name": filename,
                    "content": encoded
                })
        if(not brevo_attachments):
            send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
            to=to,
            # sender={"email": settings.DEFAULT_FROM_EMAIL, "name": "GST Web Portal"},
            sender={"email": 'sanketsawant4123@gmail.com', "name": "GST Web Portal"},
            subject=subject,
            html_content=html_content)
        
        else:
            send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
                to=to,
                # sender={"email": settings.DEFAULT_FROM_EMAIL, "name": "GST Web Portal"},
                sender={"email": 'sanketsawant4123@gmail.com', "name": "GST Web Portal"},
                subject=subject,
                html_content=html_content,
                attachment=brevo_attachments)
        response = api_instance.send_transac_email(send_smtp_email)
        print("✅ Email sent successfully:", response)
        return True
    except Exception as e:
        print("❌ Error sending email:", e)
        return False