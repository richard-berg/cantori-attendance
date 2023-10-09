import logging
from azure.identity.aio import DefaultAzureCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.email_address import EmailAddress
from msgraph.generated.models.body_type import BodyType
from msgraph.generated.models.item_body import ItemBody
from msgraph.generated.models.message import Message
from msgraph.generated.models.recipient import Recipient
from msgraph.generated.users.item.send_mail.send_mail_post_request_body import SendMailPostRequestBody

MSGRAPH_SCOPES = ["https://graph.microsoft.com/.default"]


async def send_email(subject: str, body: str, to_address: str):
    if not body:
        logging.info(f"Report '{subject}' is empty, skipping...")
        return

    async with DefaultAzureCredential() as credential:
        client = GraphServiceClient(credentials=credential, scopes=MSGRAPH_SCOPES)

    request_body = SendMailPostRequestBody(
        message=Message(
            subject=subject,
            body=ItemBody(
                content_type=BodyType.Html,
                content=body,
            ),
            to_recipients=[
                Recipient(
                    email_address=EmailAddress(
                        address=to_address,
                    ),
                ),
            ],
        ),
    )

    await client.users.by_user_id("richard.berg@cantorinewyork.com").send_mail.post(request_body)
