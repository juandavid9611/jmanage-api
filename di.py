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
from repositories.notifications.onesignal_impl import OneSignalNotificationSender
from repositories.notifications.courier_email_impl import CourierNotificationSender
from repositories.notification_repo_ddb import NotificationRepo
from repositories.notifications.ddb_impl import DdbInAppSender
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
from repositories.tournament_repo_ddb import TournamentRepo
from repositories.tournament_team_repo_ddb import TournamentTeamRepo
from repositories.tournament_player_repo_ddb import TournamentPlayerRepo
from repositories.tournament_match_repo_ddb import TournamentMatchRepo
from repositories.tournament_match_event_repo_ddb import TournamentMatchEventRepo
from services.tournament_service import TournamentService
from services.tournament_team_service import TournamentTeamService
from services.tournament_player_service import TournamentPlayerService
from services.tournament_match_service import TournamentMatchService
from services.tournament_match_event_service import TournamentMatchEventService
from services.standings_service import StandingsService
from services.tournament_stats_service import TournamentStatsService
from repositories.votation_repo_ddb import VotationRepo
from services.votation_service import VotationService


def get_notification_repo() -> NotificationRepo:
    return NotificationRepo()

def get_notification_orchestator() -> Notifications:
    email_sender = CourierNotificationSender()
    repo = get_notification_repo()
    in_app_sender = DdbInAppSender(repo=repo, onesignal=OneSignalNotificationSender())
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

# ── Tournament domain ───────────────────────────────────────────────
def get_tournament_service() -> TournamentService:
    repo = TournamentRepo()
    match_repo = TournamentMatchRepo()
    standings_svc = StandingsService(match_repo)
    team_repo = TournamentTeamRepo()
    return TournamentService(repo, standings_service=standings_svc, team_repo=team_repo, match_repo=match_repo)

def get_tournament_team_service() -> TournamentTeamService:
    repo = TournamentTeamRepo()
    tournament_repo = TournamentRepo()
    return TournamentTeamService(repo, tournament_repo=tournament_repo)

def get_tournament_player_service() -> TournamentPlayerService:
    repo = TournamentPlayerRepo()
    match_repo = TournamentMatchRepo()
    event_repo = TournamentMatchEventRepo()
    return TournamentPlayerService(repo, match_repo, event_repo)

def get_match_service() -> TournamentMatchService:
    repo = TournamentMatchRepo()
    event_repo = TournamentMatchEventRepo()
    return TournamentMatchService(repo, event_repo)

def get_match_event_service() -> TournamentMatchEventService:
    repo = TournamentMatchEventRepo()
    return TournamentMatchEventService(repo)

def get_standings_service() -> StandingsService:
    match_repo = TournamentMatchRepo()
    return StandingsService(match_repo)

def get_votation_service() -> VotationService:
    repo = VotationRepo()
    tour_repo = TourRepo()
    return VotationService(repo, tour_repo, get_user_service(), get_notification_orchestator())


def get_tournament_stats_service() -> TournamentStatsService:
    match_repo = TournamentMatchRepo()
    event_repo = TournamentMatchEventRepo()
    team_repo = TournamentTeamRepo()
    player_repo = TournamentPlayerRepo()
    return TournamentStatsService(match_repo, event_repo, team_repo, player_repo)