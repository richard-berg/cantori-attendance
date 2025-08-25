from azure.identity.aio import DefaultAzureCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.email_address import EmailAddress
from msgraph.generated.models.body_type import BodyType
from msgraph.generated.models.item_body import ItemBody
from msgraph.generated.models.message import Message
from msgraph.generated.models.recipient import Recipient
from msgraph.generated.users.item.send_mail.send_mail_post_request_body import SendMailPostRequestBody

from report_utils import Email

MSGRAPH_SCOPES = ["https://graph.microsoft.com/.default"]


async def send_email(email: Email):
    async with DefaultAzureCredential() as credential:
        client = GraphServiceClient(credentials=credential, scopes=MSGRAPH_SCOPES)

    to_recipients = [Recipient(email_address=EmailAddress(address=a)) for a in email.to]
    cc_recipients = [Recipient(email_address=EmailAddress(address=a)) for a in email.cc]

    request_body = SendMailPostRequestBody(
        message=Message(
            subject=email.subject,
            body=ItemBody(
                content_type=BodyType.Html,
                content=email.body,
            ),
            to_recipients=to_recipients,
            cc_recipients=cc_recipients,
        ),
    )

    await client.users.by_user_id("richard.berg@cantorinewyork.com").send_mail.post(request_body)
