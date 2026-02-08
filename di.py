import os
import boto3
from repositories.cognito_idp_actions import CognitoIdentityProviderWrapper
from repositories.workspace_repo_ddb import WorkspaceRepo
from services.tour_service import TourService
from services.user_service import UserService
from repositories.s3_adapter import S3Adapter
from repositories.tour_repo_ddb import TourRepo
from repositories.user_repo_ddb import UserRepo
from services.calendar_service import CalendarService
from repositories.calendar_repo_ddb import CalendarRepo
from services.notification_orchestator import Notifications
from services.payment_request_service import PaymentRequestService
from repositories.payment_requests_repo_ddb import PaymentRequestsRepo
from repositories.notifications.magicbell_impl import MagicBellNotificationSender
from repositories.notifications.courier_email_impl import CourierNotificationSender
from services.workspace_service import WorkspaceService
from services.product_service import ProductService
from repositories.product_repo_ddb import ProductRepo
from services.order_service import OrderService
from repositories.order_repo_ddb import OrderRepo
from services.membership_service import MembershipService
from repositories.membership_repo_ddb import MembershipRepo
from repositories.account_repo_ddb import AccountRepo
from services.account_service import AccountService
from services.file_service import FileService
from repositories.file_repo_ddb import FileRepo


def get_notification_orchestator() -> Notifications:
    email_sender = CourierNotificationSender()
    in_app_sender = MagicBellNotificationSender()
    return Notifications(email_sender=email_sender, in_app_sender=in_app_sender)

def get_cognito_wrapper() -> CognitoIdentityProviderWrapper:
    return CognitoIdentityProviderWrapper(
        boto3.client("cognito-idp"), os.environ.get("USER_POOL_ID"), os.environ.get("USER_POOL_API_CLIENT_ID")
    )

def get_payment_request_service() -> PaymentRequestService:
    repo = PaymentRequestsRepo()
    s3 = S3Adapter()
    return PaymentRequestService(repo, s3, get_notification_orchestator())

def get_calendar_service() -> CalendarService:
    repo = CalendarRepo()
    s3 = S3Adapter()
    tour_svc = get_tour_service()
    user_svc = get_user_service()
    return CalendarService(repo, s3, get_notification_orchestator(), tour_svc=tour_svc, user_svc=user_svc)

def get_tour_service() -> TourService:
    repo = TourRepo()
    s3 = S3Adapter()
    return TourService(repo, s3, get_notification_orchestator())

def get_user_service() -> UserService:
    repo = UserRepo()
    s3 = S3Adapter()
    cog_wrapper = get_cognito_wrapper()
    tour_svc = get_tour_service()
    membership_svc = get_membership_service()
    # Pass getter function to avoid circular dependency for account_service
    return UserService(
        repo, 
        s3, 
        get_notification_orchestator(), 
        cog_wrapper, 
        tour_svc=tour_svc, 
        membership_svc=membership_svc, 
        get_account_svc=get_account_service
    )

def get_workspace_service() -> WorkspaceService:
    repo = WorkspaceRepo()
    membership_svc = get_membership_service()
    return WorkspaceService(repo, membership_svc=membership_svc)


def get_product_service() -> ProductService:
    repo = ProductRepo()
    s3 = S3Adapter()
    return ProductService(repo, s3)

def get_order_service() -> OrderService:
    repo = OrderRepo()
    return OrderService(repo)

def get_membership_service() -> MembershipService:
    repo = MembershipRepo()
    return MembershipService(repo)

def get_account_service():
    repo = AccountRepo()
    membership_svc = get_membership_service()
    return AccountService(repo, membership_svc)

def get_file_service() -> FileService:
    repo = FileRepo()
    s3 = S3Adapter()
    return FileService(repo, s3)