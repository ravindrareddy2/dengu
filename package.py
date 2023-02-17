from __future__ import annotations

import copy
import logging
from collections import OrderedDict
from datetime import datetime
from random import randrange
from typing import Any, Dict, List, Optional, Type
from uuid import uuid4

from django import forms
from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Q
from django.db.models.base import Model
from django.template.context_processors import csrf
from django.template.loader import render_to_string
from django.utils import translation
from django.utils.functional import cached_property
from django.utils.safestring import mark_safe
from django.utils.timezone import make_aware, get_default_timezone
from django.utils.translation import ugettext as _, ugettext_lazy
from django.utils.translation import ugettext_lazy as l_
from common.clients.bss import BSSClient

import customers
from applications.fourpane.actions import TemplateAction
from applications.fourpane.forms import FourpaneForm
from applications.fourpane.handlers import RenderableChange, UpdateHandler
from applications.fourpane.models import Memo
from applications.fourpane.utils import BlockingConditionError
from applications.livesettings.models import Option
from applications.mnp.constants import (  # noimportqa
    PORTOUT_BLOCKING_STATES,
    PORTOUT_DONOR_ACCEPTED,
)
from applications.notifications.process_notification_ids import (
    ADDITIONALNUM_ACTIVATE,
    ADDITIONAL_NUMBER_CODE,
    ADDITIONAL_NUMBER_POST_CODE,
    ADDITIONAL_NUMBER_12UAH_POST_CODE,
    ADDITIONAL_NUMBER_30UAH_POST_CODE,
    ADDITIONAL_NUMBER_CHANGE_SUCCESS,
    ADDITIONAL_NUMBER_CHANGE_FAIL,
    POP_FTTB_TVPACKS_ORDER,
    DEFAULT_ACTIVATION_TEMPLATE_ID,
    POP_PROMOPACKXMB_FAILURE_RECONNECTING,
    POP_PROMOPACKXMB_AUTOMATIC_CONNECTION,
    PROMO5000MB_CODE,
    POP_FTTB_TVPACK_IVI_DEACTIVATION,
    POP_TP_ALL_FOR_300_CODE,
    FMC_HOMETV_TVPACKS_ID,
    TEMPLATE_CREDITTRANSFER_ACTIVATION,
    DIRNUM40PSTN10_CODE,
    DIRNUM0PSTN10_CODE,
    DIRNUM40PSTN30_CODE,
    DIRNUM0PSTN30_CODE,
    POP_DIRECTNUMBER_APPLICATION_TO_CONNECT_OR_CHANGE_NUMBER,
    POP_DIRECTNUMBER_REQUEST_TO_CHANGENUM_FAIL_TECHISSUE,
    POP_FTTB_TVPACK_IVI_DEACTIVATION_FAIL_UNKNOWN_ERROR,
    TEMPLATE_POP_FTTB_TVPACK_ORDER_TO_CHANGE,
    TEMPLATE_POP_FTTB_TVPACK_ORDER_TO_CHANGE_COMPLETED,
    TEMPLATE_POP_FTTB_TVPACK_ORDER_TO_CHANGE_FAIL,
    TEMPLATE_POP_FTTB_TVPACK_ORDER_TO_CHANGE_NO_INTERNET,
    TEMPLATE_POP_FTTB_TVPACK_ORDER_TO_CHANGE_NO_FUNDS,
    APPCLUB_CODE,
    CHATETATET_CODE,
    DEFAULT_REACTIVATION_TEMPLATE_ID,
    TEMPLATE_POP_FTTB_TVPACK_ACTIVATION_FAIL_NO_FIRST_PAYMENT,
    HOMETV_DEACTIVATION_WITH_KINOHIT,
    HOMETV_DEACTIVATION_WITHOUT_KINOHIT,
    DATINGCLUB_CODE,
    MUSIC_CLUB_LITE_CODE,
    MUSIC_CLUB_PREMIUM_CODE,
    POP_CHATTETATET_CODE,
    TRUSTEE_CREDIT_CODE,
    POP_AMEDIATEKA_CODE,
    ADDITIONAL_NUMBER_5UAH_POST_CODE,
    POP_ADDITIONAL_NUMBER_CHANGE_SUCCESS,
    POP_ADDITIONAL_NUMBER_CHANGE_FAIL,
    POP_ADDITIONAL_NUMBER_SUSPENSION_SUCCESS,
    POP_ADDITIONAL_NUMBER_SUSPENSION_FAILURE,
    DEFAULT_DEACTIVATION_TEMPLATE_ID,
    DEFAULT_ACTIVATION_FAILURE_TEMPLATE_ID,
    DEFAULT_DEACTIVATION_FAILURE_TEMPLATE_ID,
    TEMPLATE_FOR_B2B_PACKAGE,
    POP_MUSIC_LITE_CODE,
    POP_MUSIC_PREMIUM_CODE,
    POP_APPCLUB_CODE,
    NEW_LBS_OFFER_CODE,
    FAXTOEMAIL_CHANGE_SERVICE_FAILURE,
    FAXTOEMAIL_CHANGE_SERVICE_SUCCESS,
    ALWAYS_TOGETHER_ACTIVATION,
    FAF_ID_1_SERVICE_ATTRIBUTE,
    POP_ADDITIONAL_NUMBER_REACTIVATION_AFTER_SUSPENSION_SUCCESS,
    POP_ADDITIONAL_NUMBER_REACTIVATION_AFTER_SUSPENSION_FAILURE,
    POP_DATING_CLUB_CODE,
    ADDITIONAL_NUMBER_50UAH_POST_CODE,
    DAY_ONLINE_1KMB_OFFER_CODE,
    DAY_ONLINE_1KMB_ACTIVATION_TEMPLATE,
    CHILD_FINDER_50UAH_CODE,
)
from catalog.billing.models import BillingAccountType
from catalog.choices import (
    PACKAGE_CODE_FMC_GIGABIT,
    PACKAGE_CODE_FTTB_GIGABIT,
    PACKAGE_TAG_FMC_GIGABIT_PAID,
    PACKAGE_TAG_FTTB_GIGABIT_PAID,
    PACKAGE_TAG_LOAN,
    PACKAGE_TAG_FINANCIAL_SERVICES,
    PACKAGE_TAG_FTTB_CMSANDCS,
    PACKAGE_TAG_FTTB_CMSANDCS_ACTIVATION,
    PACKAGE_TAG_SIM_PAIR,
    POP_FTTB_TVPACKS_CHOICES,
    FMC_HOMETV_TVPACKS_CHOICES,
    PACKAGE_TAG_ALLOWED_FOR_SUSPENSION,
    PACKAGE_TAG_UNSUSPEND_BEFORE_ACTIVATION,
    PACKAGE_TAG_RBT,
    PACKAGE_TAG_CHANGE_DISCOUNT,
    FMCKYIVTVPACKIVI,
    INTERNETSIMPAIR,
    SBGIGABIT,
    SBINTSIMPAIR,
    SBSIM2NDMOB,
    VISITSERVICE,
    VISITCONFIRMED,
    PACKAGE_TAG_CMS_SERVICES,
    PRODUCT_TYPE_11,
    FTTBKYIVTVPACKIVI,
    PACKAGE_TAG_COMBINED_CHILD,
    WEEK_OF_TRUST,
    WEEK_OF_TRUST_IDENTIFIER,
    FTTBHMTVKINOHIT,
    PACKAGE_TAG_SKIPBALANCE_TRIAL_DURATION,
    TRIAL_DURATION,
    PRODUCT_FAMILY_HOMETV,
    PAUSE_IDENTIFIER,
    PACKAGE_RELATION_AVAILABLE_WITH_EXCEPTIONS,
    PACKAGE_TAG_ROUTER_DEPENDENCY,
    PACKAGE_TAG_ROUTERS_FMC_POP,
    PACKAGE_TAG_FAX_TO_EMAIL,
    PACKAGE_TAG_ACTIVATE_NEXT_MIDNIGHT,
    PACKAGE_TAG_SEPARATE_ACCOUNT,
    SERVICE_ATTRIBUTE_PAYER,
    SERVICE_ATTRIBUTE_PAYER_CHOICE_CUSTOMER,
    B2B_MONTH_DISCOUNT_PERCENT_PACKS_CHOICES,
    PACKAGE_TAG_VPBX_FIX_TRYBUY,
    PACKAGE_TAG_SKIP_BARRING_CHECK,
    PACKAGE_TAG_FORCE_STC,
    WOT_PACKAGES,
    CONFLICT_TYPE_PREVENT,
    CONFLICT_TYPE_SKIP,
    CONFLICT_TYPE_REPLACE,
)
from catalog.constants import PACKAGE_CODE_POPFMCADDON
from catalog.offers.models import DiscountPlan
from catalog.products import models as pc
from catalog.products.models import (
    Multiplier,
    Package,
    BarringRelation,
    ProductFamily,
    BarringLevel,
    PackageRelation,
    PackageConflictingRules,
    ProvisioningParameterValue,
)
from common.adapters.package_services import PackageServiceAdapter
from common.adapters.task import TaskBuilder, TaskAdapterSerializingException, TaskNode
from common.choices import (
    SCHEDULE_TIME_CHOICES,
    BARRING_FTTB_LIFECYCLE,
    BAR_AAAVIASAT,
    SCHEDULE_TIME_FIRST_OF_MONTH_AND_IMMEDIATE,
    BAR_AAA,
    BAR_VIASAT,
    SCHEDULE_TIME_NEXT_MIDNIGHT,
    SCHEDULE_TIME_FIRST_OF_MONTH_ONLY,
    SCHEDULED_FOR_SPECIFIC_DATE,
    BARRING_VOLUNTARY_SUSPENSION,
)
from common.clients.cs import CsGeneralError
from common.clients.dmp_kafka import DMP
from common.clients.errands import ErrandsClient
from common.clients.inventory import Inventory
from common.constants import (
    FIRST_REFILL_TAG,
    IS_REMOTE_INTERNET_SIM_PAIR_ACTIVATION,
    INTERNET_SIMPAIR_PACKAGE_CODE,
    INTERNET_SIMPAIR_SELF_ACTIVATE,
    INTERNET_SIMPAIR_REQUESTCONFIRM,
    INTERNET_SIMPAIR_DEACTIVATION,
    INT_SIMPAIR_EXTRA_PACKAGE_CODE,
    INT_SIMPAIR_POS_PACKAGE_CODE,
    INTERNET_SIMPAIR_PREINSTALL_ACTIVE,
    INTERNET_SIMPAIR_CHANGE,
    INTERNET_SIMPAIR_PREINSTALL_FAIL,
    NOTIFY_ME_DAYMODE_SUCCESS,
    NOTIFY_ME_FREELINE_SUCCESS,
    NOTIFY_ME_FREELINE_FAILURE,
    NOTIFY_ME_DAYMODE_FAILURE,
    NOTIFY_ME_FREELINE_DEACTIVATE,
    NOTIFY_ME_DAYMODE_DEACTIVATE,
    NOTIFY_ME_FREELINE_DEACTIVATE_FAILURE,
    NOTIFY_ME_MODIFY,
    NOTIFY_ME_MODIFY_FAILURE,
    NOTIFYME_SERVICE,
    TVSERVICE_SERVICE,
    DEFAULT_AP_COUNT,
    DEFAULT_STB_COUNT,
    TVTUNER_SERVICE,
    ADDITIONALSCREEN_SERVICE,
    DEVICE_SERVICE,
    NOTIFY_ME_LIST,
    CREDITTRANSFER_CODE,
    FREE_LINE_ATTRIBUTE,
    DAY_MODE_ATTRIBUTE,
    WTTX_CONFIRMED_ATTRIBUTE,
    WTTX_SERVICE_CODE,
    OPTION82_SERVICE,
    INSUFFICIENT_BALANCE_FAILURE_REASON,
    STATE_WAITING_FOR_ACTIVATE,
    SERVICE_ATTRIBUTE_PRODUCT_STATUS,
    FTTB_OPTION82_PACKAGE,
    WTTX_PREORDER_EVENT_ID,
    OFFER_PRICE_EXCEEDED_FAILURE_REASON,
    LINE_TYPE_FTTB,
    SERVICE_ATTRIBUTE_CMS_ID,
    ATTRIBUTE_CREDIT_LIMIT,
    TRUSTEE_CREDIT_SERVICE,
    POPFMCMERGE_SERVICE,
    ALWAYSTOGETHER_SERVICE_CODE,
    STATIC_IP_FIELD,
    STATIC_IP_SESSION_TO_FIELD,
    SELF_SIM_CHANGE,
    NOCHANGE_TVSERVICE,
    VPBX_FIX_TRYBUY_SCHEDULED_DAYS,
    PACKAGE_MAX_CHANGE_SCHEDULABLE_YEARS,
    VIRTUAL_MSISDN_ATRRIBUTE,
    CORPORATEVPN_SERVICE,
    STATIC_IP_ADDRESS_ATTRIBUTE,
    APN_ID_ATTRIBUTE,
    FIXEDSERPAUSE_PACKAGE_CODE,
    FTTBSERPAUSE_PACKAGE_CODE,
    DISCSERPAUSE_SERVICE,
    DISCOUNTPLANCODES_SERVICE_ATTRIBUTE,
    PILOT_TO_ADDITIONAL_PKG_MAPPING,
    STATIC_IP_ATTRIBUTE,
    ADDITIONALNUMBER_SERVICE,
    STATE_ACTIVATE,
)
from common.baseapi.exceptions import BaseApiExceptionCodes
from common.clients.fttbcms import FttbCMSClient
from common.utils import datetime_now, end_of_this_month
from common.utils.balance import PrepaidUtil
from common.utils.date import (
    add_to_date,
    first_of_next_month_local_with_offset,
    parse_iso_date_to_localtime,
    today,
    end_of_day,
    parse_datetime,
    first_of_next_month_local,
    morning,
    stc_end_of_day,
)
from common_base.rpc.rest import HttpErrorResponse
from customers.choices import (
    ACTIVE,
    INACTIVE,
    RECURRENCE_STATUS_WAIT_FOR_ACTIVATION,
    RECURRENCE_STATUS_PRE_ACTIVATION,
)
from customers.constants import (
    PACKAGE_INFO_LIFECYCLE_EXPIRY,
    PACKAGE_INFO_PACKAGE_EXPIRY,
    PACKAGE_INFO_RECURRENCE_STATUS,
    PACKAGE_INFO_REACTIVATION_COUNT,
    PACKAGE_INFO_FEE,
    SUBSCRIPTION_INFO_AP_COUNT,
    SUBSCRIPTION_INFO_STB_COUNT,
    MONTHLY_SCHEDULED_OFFSET_IN_MINUTES,
    SERVICE_ATTRIBUTE_VALUE_CANCEL,
    CMS_CASE_CLOSED,
    STATUS_QUARANTINE,
    STATUS_IN_USE,
)
from customers.contracts import models as cm, task_adapters
from customers.contracts.adapter_registry import (
    PackageTaskAdapterRegistry as PTARegistry,
)
from customers.contracts.constants import (
    BILLING_ACCOUNT_TYPE_SEPARATE_ACCOUNT,
    PROVISIONING_PARAM_MAPPED_DISCOUNT_PACKAGE,
)
from customers.contracts.db.cms import SubscriptionCmsOrders
from customers.contracts.db.fmc import FmcSubscriptionLink
from customers.contracts.features.billingaccount.task_adapters import (
    BillingAccountCreateCMAdapter,
)
from customers.contracts.features.option82 import Option82Handler
from customers.contracts.features.wait_for_confirmation import (
    WaitForConfirmationFormMixin,
    WaitForConfirmationHandlerMixin,
)
from customers.contracts.features.force_stc import (
    ForceSTCUtility,
    ForceSubscriptionTypeHandler,
)
from customers.contracts.features.vlr_status import GetVLRStatusHandler
from customers.contracts.form.packages.corporate_vpn_utils import CorporateVpnUtils
from customers.contracts.form.packages.spare_sim_utils import SpareSimPackageUtil
from customers.contracts.features.barring_task_adapter import SubscriptionBarringAdapter
from customers.contracts.form.packages.static_ip_package_util import (
    StaticIPServicePackageUtil,
)
from customers.contracts.form.packages.fax_to_email_utils import FaxToEmailUtil
from customers.contracts.features.direct_number import DirectNumberUtil
from customers.contracts.features.friends_and_family import FriendsAndFamilyUtil
from customers.contracts.features.fttb.utils import FTTBUtil, MasterServiceUtil
from customers.contracts.features.package_activation_adapter import (
    PackageActivationEligibilityCheckAdapter,
)
from customers.contracts.features.pontis_discount.utils import PontisDiscountPackageUtil
from customers.contracts.features.sms import notification_sms_config, SendSMSAdapter
from customers.contracts.features.stock import StockOutChangeSimHandler
from customers.contracts.form.packages.package_utils import (
    PackageUtil,
    AdditionalNumberPackageUtil,
    DirectNumberPackageUtil,
    FAXORCDMAPackageUtil,
)
from customers.contracts.forms import ReasonSelectFormMixin, MultiplierPackageFormMixin
from customers.contracts.logic.ba_balance_util import BillingAccountBalanceUtil
from customers.contracts.logic.notification import (
    NotificationUtil,
    package_disconnection_date,
)
from customers.contracts.logic.package import (
    PackageTaskAdapterAdder,
    PersistPackageChangeUtil,
    ChildPackageUtil,
)
from customers.contracts.logic.subscription import (
    SubscriptionUtil,
    SubscriptionPackageEligibilityUtil,
    SubscriptionPackageEligibilityException,
)
from customers.contracts.logic.service_attrs import serialize_services
from customers.contracts.models import (
    Subscription,
    Contract,
    SubscriptionBannedPackages,
    SubscriptionRelation,
)
from customers.contracts.tabs.billing_account.utils import (
    get_separate_billing_account_data,
)
from customers.contracts.task_adapters import OfferAdapterUtil
from customers.contracts.utils import (
    BarringEligibilityCheck,
    get_attr_value_by_language,
    get_package_price,
    update_subscription_in_case_management,
)

logger = logging.getLogger(__name__)


class PackageSetFailureNotificationSMSAdapter(task_adapters.TaskAdapter):
    task_type = 'send_sms'

    @staticmethod
    def serialize(change: dict, subscription: cm.Subscription) -> dict:
        package: Package = change.get('package')
        package_msg_config = package.message_manager_config
        if package_msg_config and package_msg_config.activation_failure_template_id:
            language = subscription.language
            return {
                'language': subscription.language,
                'msisdn': subscription.msisdn,
                'template': package_msg_config.activation_failure_template_id,
                'parameters': {
                    'package_commercial_name': package.commercial_name.get(language)
                },
            }
        return {}


class PackageHandler(WaitForConfirmationHandlerMixin, UpdateHandler):
    model = cm.Subscription
    name = 'services'
    newtlet = 'services'
    blocking_conditions = [
        'terminated',
        'sim_card_request_ongoing',
        'msisdn_request_ongoing',
        'termination_request_ongoing',
        'termination_scheduled_request_ongoing',
    ]

    def ignore_redis_task(self, memo) -> bool:
        pkg_code, (_, _) = memo.change.get("packages").popitem()
        return (
            super(PackageHandler, self).ignore_redis_task(memo)
            or pkg_code == FIXEDSERPAUSE_PACKAGE_CODE
        )

    def serialize_for_task(self, memo):
        """
        Serialize the package activation/removing/update request task
        data. The package activation/re-activation/update/removal
        provisioning and RBS requests are performed first and only
        after that the possible SMS is sent.

        :param Memo memo: The related memo instance.
        :return: Serialized data.
        :rtype: dict.
        """
        change = memo.change
        subscription: cm.Subscription = memo.instance
        pkg_code, (old_vals, new_vals) = change.get("packages").popitem()
        send_services = change.get("send_services", True)
        set_provisioning = change.get("provisioning", True)
        delete_only = change.get("delete_only", False)
        task_builder = TaskBuilder(instance=subscription)
        wait_for_confirmation_node = self._include_wait_for_confirmation_task(
            task_builder, memo
        )
        if delete_only:
            return {'tasks': task_builder.serialize(), 'msisdn': memo.instance.msisdn}
        self._crammer_checking_call(
            subscription=subscription, package=pkg_code, memo=memo
        )
        extra_data = {}
        fmc_package_details = {}
        if change.get("fmc_subscriber_msisdn"):
            fmc_package_details = self.get_fmc_package_values(memo)
            package_details = {}
            if fmc_package_details:
                package_details = list(fmc_package_details.items())[0][1]
            extra_data = {
                "activation_price": "0",
                "price": "0",
                "fmc_package_details": package_details,
            }

        cms_node = task_builder.add_node(parent_node=wait_for_confirmation_node)
        pre_act_node = task_builder.add_node(parent_node=cms_node)
        act_node = task_builder.add_node(parent_node=pre_act_node)
        rbs_node = task_builder.add_node(parent_node=act_node)
        sfa_node = task_builder.add_node(parent_node=rbs_node)
        sms_node = task_builder.add_node(parent_node=act_node)

        package = OfferAdapterUtil.validate_and_get_package(
            package_code=pkg_code, action='setting', subscription_id=memo.instance.id
        )

        # Eligibility check for package activation
        # when the subscription is in the suspended status.
        if new_vals is not None:
            if all(
                [
                    not subscription.validate_package_allowed_for_suspended(pkg_code),
                    not package.has_tag(PACKAGE_TAG_UNSUSPEND_BEFORE_ACTIVATION),
                ]
            ):
                raise TaskAdapterSerializingException(
                    f"Package {pkg_code} can not be activated as "
                    "the subscription is in suspended status."
                )

        if old_vals is not None and new_vals is not None:
            # package is re-activated or only service attributes are changed
            if package.re_activation:
                self.validate_package_cardinality(memo.instance, package)
                self.validate_package_cardinality_variant(memo.instance, package)
            self.validate_package_eligibility(memo.instance, package)
            if change.get('assign_current_subscription', True):
                PackageUtil.validate_option82_change(
                    new_vals.get("OPTION82", {}).get("username"), change
                )
            if package.has_tag(PACKAGE_TAG_SIM_PAIR):
                set_provisioning = False
            re_act = bool(change.get('re_activated'))
            PackageTaskAdapterAdder.append_package_activation_tasks(
                pre_prov_node=pre_act_node,
                prov_node=act_node,
                rbs_node=rbs_node,
                cms_node=cms_node,
                other_node=sms_node,
                memo=memo,
                new_packages={pkg_code: new_vals},
                activation_type=[
                    PTARegistry.PACKAGE_SERVICE_UPDATE,
                    PTARegistry.PACKAGE_REACTIVATION,
                ][re_act],
                logger=self.logger,
                send_services=send_services,
                set_provisioning=set_provisioning,
                check_cardinality=package.re_activation,
                extra_changes=extra_data,
                service_value_update=True if not package.re_activation else False,
            )
        else:
            if old_vals is not None:
                PackageTaskAdapterAdder.append_package_removing_tasks(
                    pre_prov_node=pre_act_node,
                    prov_node=act_node,
                    rbs_node=rbs_node,
                    sfa_node=sfa_node,
                    cms_node=cms_node,
                    other_node=sms_node,
                    memo=memo,
                    old_packages={pkg_code: old_vals},
                    logger=self.logger,
                    send_services=send_services,
                    set_provisioning=set_provisioning,
                )
            elif new_vals is not None:
                # package is added
                package_key = 'pk' if pkg_code.isdigit() else 'code'
                package = Package.objects.get(Q(**{package_key: pkg_code}))
                self.validate_balance_check(subscription, package, memo)
                barrings = self._currently_active_provisionable_suspension_barrings(
                    subscription, package
                )
                if barrings:
                    _change = memo.change
                    _change["barrings"] = barrings
                    memo.change = _change
                    memo.save()
                if package.has_tag(PACKAGE_TAG_SEPARATE_ACCOUNT):
                    reserved_billing_account_id = Inventory.reserve_available_billing_account_number(
                        actor=f"separate_account_{subscription.contract.confirmation_code}"
                    ).get(
                        "billing_account"
                    )
                    _change = memo.change
                    _change['reserved_billing_account_id'] = reserved_billing_account_id
                    memo.change = _change
                    memo.save()
                    # BA will be also sent to RBS within this task
                    ba_type = BillingAccountType.objects.get(
                        code=BILLING_ACCOUNT_TYPE_SEPARATE_ACCOUNT
                    )
                    service_code, service_attributes = list(new_vals.items())[0]
                    payer_attribute = service_attributes.get(SERVICE_ATTRIBUTE_PAYER)
                    if payer_attribute == SERVICE_ATTRIBUTE_PAYER_CHOICE_CUSTOMER:
                        owner_id = payer_id = subscription.user.id
                    else:
                        owner_id = payer_id = subscription.owner.id

                    billing_acount = subscription.non_terminated_billing_account
                    if billing_acount:
                        change["billing_account"] = get_separate_billing_account_data(
                            billing_acount
                        )

                    pre_act_node.add_task(
                        task_class=BillingAccountCreateCMAdapter,
                        change={
                            **change,
                            **{
                                'extra': {
                                    'ba_type': ba_type,
                                    'owner_id': owner_id,
                                    'payer_id': payer_id,
                                    'status': ACTIVE,
                                    'ba_id': reserved_billing_account_id,
                                    'skip_account_group_update': True,
                                }
                            },
                        },
                        inform_publisher=True,
                    )

                if change.get("fmc_subscriber_msisdn"):
                    if package.has_service(OPTION82_SERVICE):
                        send_services = True
                    else:
                        send_services = False
                self.validate_package_cardinality(memo.instance, package)
                self.validate_package_eligibility(memo.instance, package)
                if change.get('assign_current_subscription', True):
                    PackageUtil.validate_option82_change(
                        new_vals.get("OPTION82", {}).get("username"), change
                    )
                self.validate_package_cardinality_variant(memo.instance, package)

                if (
                    pkg_code in POP_FTTB_TVPACKS_CHOICES
                    or pkg_code in FMC_HOMETV_TVPACKS_CHOICES
                ) and memo.change['is_scheduled']:
                    self.validate_tv_package_balance_check(memo, package)
                # package is re-activated or activated
                re_act = bool(change.get('re_activated'))
                PackageTaskAdapterAdder.append_package_activation_tasks(
                    pre_prov_node=pre_act_node,
                    prov_node=act_node,
                    rbs_node=rbs_node,
                    sfa_node=sfa_node,
                    cms_node=cms_node,
                    other_node=sms_node,
                    memo=memo,
                    new_packages={pkg_code: new_vals},
                    activation_type=[
                        PTARegistry.PACKAGE_ACTIVATION,
                        PTARegistry.PACKAGE_REACTIVATION,
                    ][re_act],
                    logger=self.logger,
                    send_services=send_services,
                    set_provisioning=set_provisioning,
                    barrings=barrings,
                    extra_changes=extra_data,
                )
        if old_vals is None:
            if pkg_code in NOTIFY_ME_LIST and new_vals:
                if new_vals.get(NOTIFYME_SERVICE, {}).get(DAY_MODE_ATTRIBUTE) == 'YES':
                    self.send_sms_notification(
                        memo, pkg_code, NOTIFY_ME_DAYMODE_SUCCESS, sms_node
                    )
                elif (
                    new_vals.get(NOTIFYME_SERVICE, {}).get(FREE_LINE_ATTRIBUTE) == 'YES'
                ):
                    self.send_sms_notification(
                        memo, pkg_code, NOTIFY_ME_FREELINE_SUCCESS, sms_node
                    )
            elif pkg_code in [
                ADDITIONAL_NUMBER_CODE,
                ADDITIONAL_NUMBER_POST_CODE,
                ADDITIONAL_NUMBER_12UAH_POST_CODE,
                ADDITIONAL_NUMBER_30UAH_POST_CODE,
                ADDITIONAL_NUMBER_5UAH_POST_CODE,
                ADDITIONAL_NUMBER_50UAH_POST_CODE,
            ]:
                self.send_sms_notification(
                    memo, pkg_code, ADDITIONALNUM_ACTIVATE, sms_node
                )
            elif pkg_code in [CHATETATET_CODE, DATINGCLUB_CODE, APPCLUB_CODE]:
                if (
                    subscription.xref_packages.filter(  # type: ignore
                        package__code=pkg_code
                    ).count()
                    > 1
                ):
                    self.send_sms_notification(
                        memo, pkg_code, DEFAULT_REACTIVATION_TEMPLATE_ID, sms_node
                    )
        elif old_vals and new_vals:
            if pkg_code == INT_SIMPAIR_POS_PACKAGE_CODE:
                self.send_sms_notification(
                    memo, pkg_code, INTERNET_SIMPAIR_CHANGE, sms_node
                )
            elif pkg_code in NOTIFY_ME_LIST:
                self.send_sms_notification(memo, pkg_code, NOTIFY_ME_MODIFY, sms_node)
            elif pkg_code == ADDITIONAL_NUMBER_CODE:
                if AdditionalNumberPackageUtil.get_virtual_msisdn(new_vals, old_vals):
                    self.send_sms_notification(
                        memo, pkg_code, ADDITIONAL_NUMBER_CHANGE_SUCCESS, sms_node
                    )
            elif package.has_service(DEVICE_SERVICE):
                self.send_sms_notification(
                    memo, pkg_code, DEFAULT_ACTIVATION_TEMPLATE_ID, sms_node
                )
        elif new_vals is not None:
            if (
                pkg_code in NOTIFY_ME_LIST
                and (new_vals.get(pkg_code)).get(FREE_LINE_ATTRIBUTE) == 'YES'
            ):
                self.send_sms_notification(
                    memo, pkg_code, NOTIFY_ME_FREELINE_DEACTIVATE, sms_node
                )
            elif (
                pkg_code in NOTIFY_ME_LIST
                and (new_vals.get(pkg_code)).get(DAY_MODE_ATTRIBUTE) == 'YES'
            ):
                self.send_sms_notification(
                    memo, pkg_code, NOTIFY_ME_DAYMODE_DEACTIVATE, sms_node
                )
        else:
            if pkg_code in NOTIFY_ME_LIST and new_vals:
                if new_vals.get(NOTIFYME_SERVICE, {}).get(FREE_LINE_ATTRIBUTE) == 'YES':
                    self.send_sms_notification(
                        memo, pkg_code, NOTIFY_ME_FREELINE_DEACTIVATE, sms_node
                    )
                elif (
                    new_vals.get(NOTIFYME_SERVICE, {}).get(DAY_MODE_ATTRIBUTE) == 'YES'
                ):
                    self.send_sms_notification(
                        memo, pkg_code, NOTIFY_ME_DAYMODE_DEACTIVATE, sms_node
                    )
        if (
            new_vals
            and subscription.type.code == POP_TP_ALL_FOR_300_CODE
            and pkg_code == PROMO5000MB_CODE
        ):
            self.send_sms_notification(
                memo, pkg_code, POP_PROMOPACKXMB_AUTOMATIC_CONNECTION, sms_node
            )
        return {'tasks': task_builder.serialize(), 'msisdn': memo.instance.msisdn}

    def _crammer_checking_call(
        self, subscription: Subscription, package: Package, memo: Memo
    ) -> None:
        """
        Updating Gigabit provisioning parameter Gigabit parameter

        :param subscription: Subscription
        :param package: Package
        :param data: serialized data
        """
        change = memo.change
        skip_cramer_check = change.get('skip_cramer_check', False)
        package = Package.objects.filter(code=package).first()
        if subscription.is_fttb_subscription or subscription.is_fmc_subscription:
            if (
                package.has_tag_in(
                    [PACKAGE_TAG_FTTB_GIGABIT_PAID, PACKAGE_TAG_FMC_GIGABIT_PAID]
                )
            ) or package.code in SBGIGABIT:
                if not skip_cramer_check:
                    crammer_response = (
                        BSSClient.get_crammer_check(msisdn=subscription.msisdn)
                        .get("data", {})
                        .get("attributes", {})
                        .get("status", 0)
                    )
                    if not crammer_response:
                        customer_id = subscription.owner.pk
                        category_name_parts = [
                            "Домашний интернет",
                            "Segment FTTB",
                            "Услуги ФТТБ",
                            "Гигабит",
                            "Заявка на замену кабеля",
                            "Замена кабеля с 2 на 4 пары для Гигабита",
                            "Замена кабеля для возможности заказа ТП Гигабит",
                        ]
                        category_name = " / ".join(category_name_parts)
                        attributes = {
                            "MSISDN": subscription.msisdn,
                            "CategoryName": category_name,
                            "ChannelName": "CSRtb",
                            "Title": "Заявка на замену кабеля Замена кабеля с 2 на 4 пары для Гигабита",
                            "Author": " technical user",
                            "StartDate": datetime_now().isoformat(),
                            "VirtualTeamName": "509",
                            "ActionToResolve": "Заявка принята",
                            "ReactionName": "Автомат. обработка (CMS обслуживание)",
                            "_meta": {"channel": "CSRtb"},
                        }

                        if package.code in [
                            PACKAGE_CODE_FTTB_GIGABIT,
                            PACKAGE_CODE_FMC_GIGABIT,
                        ]:
                            prov_param = ProvisioningParameterValue.objects.filter(
                                package=package,
                                parameter__is_provisionable=True,
                                parameter__code=PROVISIONING_PARAM_MAPPED_DISCOUNT_PACKAGE,
                            ).first()
                            if prov_param:
                                pkg = Package.objects.filter(
                                    code=str(prov_param.raw_value)
                                ).first()
                                if pkg:
                                    change['packages'][str(pkg.code)] = change[
                                        'packages'
                                    ].pop(package.code)
                                    memo.change = change
                                    memo.save()
                        BSSClient.create_superoffice_ticket(
                            attributes=attributes, customer_id=customer_id
                        )

    def _currently_active_provisionable_suspension_barrings(
        self, subscription: Subscription, package: Package
    ) -> list:
        """
        Method to find the currently active provisionable barrings

        :param subscription: Subscription instance
        :param package: Package instance, to check only in WOTpackage cases
        :return: currently active barrings as a list of dict
        """
        barring_data = []
        if package.has_tag(PACKAGE_TAG_UNSUSPEND_BEFORE_ACTIVATION):
            barrings = subscription.barrings.filter(barring__provisionable=True)
            for barring in barrings:
                change_data = {
                    str(BarringLevel.LEVEL_OPERATOR): False,
                    "barring": barring.barring.code,
                }
                change_valid = SubscriptionBarringAdapter.barring_to_be_active(
                    change_data, subscription
                )
                if change_valid is False:
                    barring_data.append(change_data)
        return barring_data

    def get_fmc_package_values(self, memo: Memo) -> dict:
        """
        Method to get the old package values that is activated on fmc subscription

        :param memo: Memo instance
        :return: dict of values
        """
        data = {}
        change = memo.change
        fmc_subscriber_msisdn = change.get("fmc_subscriber_msisdn")
        if fmc_subscriber_msisdn:
            pkg_code, (old_vals, new_vals) = change.get(  # type: ignore
                "packages"
            ).popitem()
            fmc_subscription = Subscription.objects.get(
                msisdn=fmc_subscriber_msisdn, contract__status=Contract.ACTIVE
            )
            fttb_package = Package.objects.get(code=pkg_code)
            fmc_sub_pkg = fmc_subscription.xref_packages.filter(
                package=fttb_package.fmc_package
            ).latest('pk')

            data.update(
                {
                    fttb_package.code: {
                        PACKAGE_INFO_LIFECYCLE_EXPIRY: fmc_sub_pkg.deactivated_at,
                        PACKAGE_INFO_PACKAGE_EXPIRY: fmc_sub_pkg.recurrence_end_date,
                        PACKAGE_INFO_RECURRENCE_STATUS: fmc_sub_pkg.recurrence_status,
                        PACKAGE_INFO_REACTIVATION_COUNT: fmc_sub_pkg.reactivation_count,
                        PACKAGE_INFO_FEE: fmc_sub_pkg.fee,
                        SUBSCRIPTION_INFO_AP_COUNT: fmc_subscription.ap_count,
                        SUBSCRIPTION_INFO_STB_COUNT: fmc_subscription.stb_count,
                    }
                }
            )
        return data

    @staticmethod
    def validate_can_activate_child_tv_pack(
        subscription: Subscription, package: Package
    ) -> None:
        """
        Method to check if the child tv pack can be activated, the activation
        is restricted if any of the package with tag CombinedTvChildPackageTag
        is already active

        :param subscription: Subscription instance
        :param package: Package instance
        :raises: SubscriptionPackageEligibilityException: If above rule
        prevent package activation.
        """
        if package.has_tag(PACKAGE_TAG_COMBINED_CHILD):
            if subscription.xref_packages.filter(
                package__tags__code=PACKAGE_TAG_COMBINED_CHILD
            ).exists():
                raise ValidationError("Discount already provided.")

    @staticmethod
    def validate_child_sb_package(subscription: Subscription, package: Package) -> None:
        if package.code in [SBSIM2NDMOB, SBINTSIMPAIR]:
            if subscription.xref_packages.active(
                package__code__in=[SBSIM2NDMOB, SBINTSIMPAIR]
            ):
                raise ValidationError("Only one SB package will be activated at a time")

    @staticmethod
    def validate_package_cardinality(
        subscription: Subscription, package: Package
    ) -> None:
        """
        Validates the package cardinality in case there are cardinality
        rules applied to the package. It might be that he package is
        allowed to be activated only certain amount of times in certain
        time period.

        :param Subscription subscription: Consumer subscription which is
            active.
        :param Package package: The package which is being activated.
        :raises ValidationError: If cardinality rules prevent package
            activation.
        """
        OfferAdapterUtil.validate_package_cardinality(
            subscription, package, ValidationError
        )

    @staticmethod
    def validate_package_cardinality_variant(
        subscription: Subscription, package: Package
    ) -> None:
        """
        Validates the package cardinality calendar_month  in case there is
        cardinality_variant set to calendar_month.

        Checks whether subscription package active in current month or not
                :if already active  raises validation error
        Checks whether subscription package is going to active either
        in current month or in next_month
                :if not limited by the above constriant will raise the validation error.

        :param subscription: Consumer subscription which is active
        :param package: the package code, which is being activated
        """
        OfferAdapterUtil.validate_package_cardinality_variant(
            subscription, package, ValidationError
        )

    @staticmethod
    def should_skip_balance(package: Package, change):
        """
        check & verify skip balance flag.
        """
        packages = change.get("packages")
        skip_balance_check = "Status" in str(packages) and (
            STATE_WAITING_FOR_ACTIVATE in str(packages)
            or STATE_ACTIVATE in str(packages)
        )
        if skip_balance_check:
            return skip_balance_check
        if package.has_tag(PACKAGE_TAG_SKIPBALANCE_TRIAL_DURATION):
            pkg_code, (old_values, new_values) = change.get("packages").popitem()
            for service_value in new_values.values():
                for service_attr_key, service_attr_value in service_value.items():
                    if (
                        service_attr_key == TRIAL_DURATION
                        and service_attr_value
                        and int(service_attr_value) > 0
                    ):
                        return True
        return skip_balance_check

    def validate_balance_check(
        self, subscription: Subscription, package: Package, memo: Memo
    ):
        """
        Method to validate balance during package activation execution.

        :param subscription: Subscription instance
        :param package: package instance
        :param memo: memo instance
        :raise: TaskAdapterSerializingException
        """
        change = memo.change
        package_eligibility_util = SubscriptionPackageEligibilityUtil(
            subscription=subscription, package=package, actor=memo.actor
        )
        skip_balance_check = self.should_skip_balance(package, change)
        if change.get('provisioning', True) and not (
            skip_balance_check or change.get("allow_over_charge", False)
        ):
            try:
                package_eligibility_util.validate_balance_check()
            except SubscriptionPackageEligibilityException as err:
                change['balance_check_failed'] = True
                memo.change = change
                memo.save()
                raise TaskAdapterSerializingException(str(err))

    def validate_tv_package_balance_check(self, memo: Memo, package: Package):
        try:
            balance = PrepaidUtil.available_balance(
                memo.instance.msisdn, memo.instance.is_not_mobile_subscription
            )
        except HttpErrorResponse as error:
            raise ValidationError(error)
        pkg_code = package.code
        template_id = None
        if package.code in POP_FTTB_TVPACKS_CHOICES:
            template_id = TEMPLATE_POP_FTTB_TVPACK_ORDER_TO_CHANGE_NO_FUNDS
        self.account_balance = balance
        if not memo.instance.is_first_refill_done:
            template_id = TEMPLATE_POP_FTTB_TVPACK_ACTIVATION_FAIL_NO_FIRST_PAYMENT
            self.send_sms_notification(memo, pkg_code, template_id)
            message = "Package activation failed, please do first refill"
            raise SubscriptionPackageEligibilityException(
                message, code=BaseApiExceptionCodes.BALANCE_INSUFFICIENT
            )
        if package.fee_item and package.fee_item.price > self.account_balance:
            if template_id:
                self.send_sms_notification(memo, pkg_code, template_id)
            message = "Package activation failed due to insufficient funds"
            raise SubscriptionPackageEligibilityException(
                message, code=BaseApiExceptionCodes.BALANCE_INSUFFICIENT
            )

    @staticmethod
    def validate_package_eligibility(subscription: Subscription, package: Package):
        """
        1) Validate loan eligibility if package is a loan package

        2) For portout_request, validate - 'Balance Transfer cannot be
           availed when Portout is in progress'

        :param subscription: The related subscription
        :param package: Package
        :raises: ValidationError if package activation/removing/update
                eligibility fails
        """

        if package.has_tag(PACKAGE_TAG_LOAN):
            if subscription.is_temporary_msisdn:
                raise ValidationError("Loan cannot be availed for temporary msisdn")

            if subscription.portout_request in PORTOUT_BLOCKING_STATES:
                raise ValidationError(
                    "Loan cannot be availed when Portout is in progress"
                )

        if package.has_tag(
            PACKAGE_TAG_FINANCIAL_SERVICES
        ) and subscription.portout_request in (
            PORTOUT_BLOCKING_STATES + [PORTOUT_DONOR_ACCEPTED]
        ):
            raise ValidationError(
                "Balance Transfer cannot be availed when Portout is in " "progress"
            )

    def get_change_display(self, change):
        pkg, (old_vals, new_vals) = change['packages'].popitem()
        if new_vals is None:
            new_state = _('Disabled')
        elif old_vals is None:
            new_state = _('Enabled')
        else:
            new_state = _('Service parameters changed')
        result = {_('Package {}').format(pkg): new_state}
        if new_vals is not None:
            ref = new_vals
        else:
            ref = old_vals
        if old_vals is None:
            old_vals = {}
        if new_vals is None:
            new_vals = {}
        for key in ref.keys():
            # FIXME: Consider service_value unpacking from products.models
            result[f"Service {key}"] = (
                old_vals.get(key, _('off')),
                new_vals.get(key, _('off')),
            )
        if 'barrings' in change:
            result['barrings'] = change['barrings']
        if "reason" in change:
            result['reason'] = change['reason']
        return RenderableChange(result)

    @staticmethod
    def send_sms_notification(
        memo: Memo,
        pkg_code: str,
        template_name: str,
        maybe_sms_node: Optional[TaskNode] = None,
    ):
        subscription = memo.instance
        package: Package = Package.objects.get(code=pkg_code)
        package_msg_config = package.msg_manager_config.first()
        try:
            pkg_code, (old_vals, new_vals) = memo.change.get(  # type: ignore
                "packages"
            ).popitem()
        except AttributeError:
            packages = memo.change.get('packages')
            if not packages:
                return
            old_packages, new_packages = packages
            old_pkg, old_vals = old_packages.popitem()
            pkg_code, new_vals = new_packages.popitem()

        language = memo.instance.language
        values = new_vals or old_vals
        template_parameters: Dict[str, Optional[str]] = {
            'package_commercial_name': package.commercial_name.get(language),
            'subscription_id': subscription.msisdn,
            'free_line_characteristic_value': get_attr_value_by_language(
                values, NOTIFYME_SERVICE, FREE_LINE_ATTRIBUTE, language
            )
            if pkg_code in NOTIFY_ME_LIST
            else None,
            'day_mode_characteristic_value': get_attr_value_by_language(
                values, NOTIFYME_SERVICE, DAY_MODE_ATTRIBUTE, language
            )
            if pkg_code in NOTIFY_ME_LIST
            else None,
        }
        if pkg_code in [
            ADDITIONAL_NUMBER_CODE,
            ADDITIONAL_NUMBER_POST_CODE,
            ADDITIONAL_NUMBER_12UAH_POST_CODE,
            ADDITIONAL_NUMBER_30UAH_POST_CODE,
            ADDITIONAL_NUMBER_5UAH_POST_CODE,
            ADDITIONAL_NUMBER_50UAH_POST_CODE,
        ]:
            additional_msisdn = AdditionalNumberPackageUtil.get_virtual_msisdn(
                new_vals, old_vals
            )
            if additional_msisdn:
                template_parameters['additional_msisdn'] = additional_msisdn
        elif pkg_code == TRUSTEE_CREDIT_CODE:
            template_parameters['CL_softblock_threshold'] = values.get(
                TRUSTEE_CREDIT_SERVICE, {}
            ).get(ATTRIBUTE_CREDIT_LIMIT)
        elif pkg_code in B2B_MONTH_DISCOUNT_PERCENT_PACKS_CHOICES.keys():
            discount = B2B_MONTH_DISCOUNT_PERCENT_PACKS_CHOICES.get(pkg_code, '0')
            discount_expiry_date = NotificationUtil.populate_discount_expiry(new_vals)
            template_parameters.update(
                {
                    'discount': str(discount) + "%",
                    'discount_expiry_date': discount_expiry_date,
                }
            )

        if pkg_code in [
            DIRNUM40PSTN10_CODE,
            DIRNUM0PSTN10_CODE,
            DIRNUM40PSTN30_CODE,
            DIRNUM0PSTN30_CODE,
        ]:
            pstn_city_names = values["DIRECTNUMBER"]
            pstn_numbers = ",".join([str(v) for i, v in pstn_city_names.items() if v])
            template_parameters["pstn_numbers"] = pstn_numbers

        if package_msg_config:
            if template_name == 'activation_template_id':
                if subscription.is_b2b:
                    template_id = TEMPLATE_FOR_B2B_PACKAGE[
                        DEFAULT_ACTIVATION_TEMPLATE_ID
                    ].get(pkg_code, package_msg_config.activation_template_id)
                else:
                    template_id = package_msg_config.activation_template_id
            elif template_name == 'deactivation_template_id':
                if subscription.is_b2b:
                    template_id = TEMPLATE_FOR_B2B_PACKAGE[
                        DEFAULT_DEACTIVATION_TEMPLATE_ID
                    ].get(pkg_code, package_msg_config.deactivation_template_id)
                else:
                    template_id = package_msg_config.deactivation_template_id
                template_parameters.update(package_disconnection_date())
            elif template_name == 'activation_failure_template_id':
                if subscription.is_b2b:
                    template_id = TEMPLATE_FOR_B2B_PACKAGE[
                        DEFAULT_ACTIVATION_FAILURE_TEMPLATE_ID
                    ].get(pkg_code, package_msg_config.activation_failure_template_id)
                else:
                    template_id = package_msg_config.activation_failure_template_id
            elif template_name == 'deactivation_failure_template_id':
                if subscription.is_b2b:
                    template_id = TEMPLATE_FOR_B2B_PACKAGE[
                        DEFAULT_DEACTIVATION_FAILURE_TEMPLATE_ID
                    ].get(pkg_code, package_msg_config.deactivation_failure_template_id)
                else:
                    template_id = package_msg_config.deactivation_failure_template_id
            elif template_name == "activation_renewal_template_id":
                template_id = package_msg_config.activation_renewal_template_id
            elif template_name == 'insufficient_balance_to_activate_product':
                template_id = package_msg_config.insufficient_balance_template_id
                template_parameters['package_price'] = (
                    settings.MONETARY_FORMAT.format(package.fee_item.price)
                    if package.fee_item
                    else None
                )
                template_parameters['currency'] = settings.CURRENCY
                balance = PrepaidUtil.available_balance(subscription.msisdn)
                recommended_payment: Optional[str] = None
                if package.fee_item and package.fee_item.price > balance:
                    recommended_payment = str(
                        round(package.fee_item.price - balance, 2)
                    )
                template_parameters.update({'recommended_payment': recommended_payment})
            elif template_name == ADDITIONALNUM_ACTIVATE:
                template_id = package_msg_config.activation_template_id or template_name
            else:
                template_id = template_name
        else:
            template_id = template_name

        if not template_id:
            return
        if maybe_sms_node:
            maybe_sms_node.add_task(
                task_class=SendSMSAdapter,
                change={
                    'template': template_id,
                    'msisdn': NotificationUtil.get_notification_msisdn(subscription),
                    'language': language,
                    'parameters': template_parameters,
                },
            )
        else:
            notification_sms_config(
                template_id, memo.actor, subscription, template_parameters
            )

    def do_persist(self, memo: Memo):
        """
        Method to persist db changes

        :param memo: Memo instance
        """
        change = memo.change
        pkg_code, (old_vals, new_vals) = list(
            change.get(  # type: ignore
                "packages"
            ).items()
        )[0]

        delete_only = memo.change.get("delete_only", False)
        if delete_only:
            subscription = memo.instance
            subscription.xref_packages.filter(
                package__code=pkg_code,
                recurrence_status=RECURRENCE_STATUS_PRE_ACTIVATION,
            ).delete()
            if old_vals.keys():
                for sub_serv in subscription.subscriptionpackageservice_set.filter(
                    service__code=list(old_vals.keys())[0]
                ):
                    sub_serv.deactivate()
            return

        package_info = self.get_fmc_package_values(memo)
        util = PersistPackageChangeUtil(
            sub=memo.instance,
            change=change,
            changed_at=memo.executed_at,
            memo=memo,
            package_info=package_info,
        )
        util.persist()
        subscription = memo.instance
        pkg = Package.objects.only('id').prefetch_related('services').get(code=pkg_code)

        # When demo package is activated, add it to banned packed list
        if change.get("child_demo_package"):
            SubscriptionBannedPackages.objects.create(
                subscription=subscription, package=pkg, created_at=datetime_now()
            )

        # Check for active suspension barrings only on activation.
        if new_vals is not None:
            barring_changes = self._currently_active_provisionable_suspension_barrings(
                subscription, package=pkg
            )
            for barring_change in barring_changes:
                barring_code = barring_change.get('barring')
                try:
                    barring = pc.Barring.objects.get(code=barring_code)
                except pc.Barring.DoesNotExist:
                    raise
                level_data = BarringEligibilityCheck.barring_levels_from_change(
                    barring_change
                )
                for level, value in level_data.items():
                    subscription.set_barring(barring, int(level), value)
                subscription.contract.toggle_suspension()

        PackageHandler.mark_first_refill_status(subscription)

        change = memo.change

        if change.get("recurrence_status") is not None:
            util.update_recurrence_status(subscription, pkg, change)
            return

        if old_vals is None:
            self.invalidate_cache(pkg_id=pkg.id, subscription_id=subscription.id)
            if pkg_code == settings.DORMANT_PACKAGE_CODE:
                contract = subscription.contract
                dormant_subscription = subscription.dormant

                with transaction.atomic():
                    contract = Contract.objects.select_for_update.get(  # type: ignore
                        pk=contract.pk
                    )
                    if dormant_subscription and not contract.is_dormant:
                        contract.status = contract.DORMANT
                        contract.save(update_fields=['status'])
                    elif not dormant_subscription and contract.is_dormant:
                        contract.status = contract.ACTIVE
                        contract.save(update_fields=['status'])
            elif pkg_code == DAY_ONLINE_1KMB_OFFER_CODE:
                self.send_sms_notification(
                    memo, pkg_code, DAY_ONLINE_1KMB_ACTIVATION_TEMPLATE
                )
            elif pkg_code in [
                CHATETATET_CODE,
                DATINGCLUB_CODE,
                APPCLUB_CODE,
                MUSIC_CLUB_LITE_CODE,
                MUSIC_CLUB_PREMIUM_CODE,
                POP_CHATTETATET_CODE,
                POP_AMEDIATEKA_CODE,
                POP_MUSIC_LITE_CODE,
                POP_MUSIC_PREMIUM_CODE,
                POP_APPCLUB_CODE,
                NEW_LBS_OFFER_CODE,
                POP_DATING_CLUB_CODE,
                CHILD_FINDER_50UAH_CODE,
            ]:
                if (
                    subscription.xref_packages.filter(  # type: ignore
                        package__code=pkg_code
                    ).count()
                    > 1
                ):
                    self.send_sms_notification(
                        memo, pkg_code, DEFAULT_REACTIVATION_TEMPLATE_ID
                    )
        elif old_vals and new_vals:
            if pkg_code in [
                ADDITIONAL_NUMBER_POST_CODE,
                ADDITIONAL_NUMBER_12UAH_POST_CODE,
                ADDITIONAL_NUMBER_30UAH_POST_CODE,
                ADDITIONAL_NUMBER_5UAH_POST_CODE,
                ADDITIONAL_NUMBER_50UAH_POST_CODE,
            ]:
                if AdditionalNumberPackageUtil.get_virtual_msisdn(new_vals, old_vals):
                    self.send_sms_notification(
                        memo, pkg_code, POP_ADDITIONAL_NUMBER_CHANGE_SUCCESS
                    )
                elif AdditionalNumberPackageUtil.get_temporary_suspension(
                    new_vals, old_vals
                ):
                    self.send_sms_notification(
                        memo, pkg_code, POP_ADDITIONAL_NUMBER_SUSPENSION_SUCCESS
                    )
                elif AdditionalNumberPackageUtil.get_temporary_suspension_reactivation(
                    new_vals, old_vals
                ):
                    self.send_sms_notification(
                        memo,
                        pkg_code,
                        POP_ADDITIONAL_NUMBER_REACTIVATION_AFTER_SUSPENSION_SUCCESS,
                    )
            elif pkg.has_tag(PACKAGE_TAG_FAX_TO_EMAIL):
                self.send_sms_notification(
                    memo, pkg_code, FAXTOEMAIL_CHANGE_SERVICE_SUCCESS
                )
            elif pkg.has_service(DEVICE_SERVICE) or pkg.has_service(TVTUNER_SERVICE):
                self.send_sms_notification(
                    memo, pkg_code, DEFAULT_ACTIVATION_TEMPLATE_ID
                )
        else:
            if pkg_code in NOTIFY_ME_LIST:
                if old_vals.get(NOTIFYME_SERVICE, {}).get(FREE_LINE_ATTRIBUTE) == 'YES':
                    self.send_sms_notification(
                        memo, pkg_code, NOTIFY_ME_FREELINE_DEACTIVATE
                    )
                elif (
                    old_vals.get(NOTIFYME_SERVICE, {}).get(DAY_MODE_ATTRIBUTE) == 'YES'
                ):
                    self.send_sms_notification(
                        memo, pkg_code, NOTIFY_ME_DAYMODE_DEACTIVATE
                    )

        if (
            new_vals
            and subscription.type.code == POP_TP_ALL_FOR_300_CODE
            and pkg_code == PROMO5000MB_CODE
        ):
            self.send_sms_notification(
                memo, pkg_code, POP_PROMOPACKXMB_AUTOMATIC_CONNECTION
            )

        if memo.change.get("fmc_subscriber_msisdn"):
            subscription.ap_count = package_info.get(SUBSCRIPTION_INFO_AP_COUNT)
            subscription.stb_count = package_info.get(SUBSCRIPTION_INFO_STB_COUNT)
            subscription.save(update_fields=['ap_count', 'stb_count', 'last_modified'])
        else:
            self.handle_tv_service(pkg, subscription, new_vals)
            subscription.save(update_fields=['ap_count', 'stb_count', 'last_modified'])

        # on tv-tuner deactivation(by CMS), update the cms orders table
        if new_vals is None and pkg.has_tag(PACKAGE_TAG_FTTB_CMSANDCS):
            self.update_subscription_cms_status(pkg, subscription)
        if new_vals:
            for serv_code, serv_attrs in new_vals.items():
                if serv_code == ADDITIONALNUMBER_SERVICE and serv_attrs.get(
                    VIRTUAL_MSISDN_ATRRIBUTE
                ):
                    virtual_msisdn = serv_attrs[VIRTUAL_MSISDN_ATRRIBUTE]
                    Inventory.activate_msisdn(virtual_msisdn)
        subscription.save(update_fields=['ap_count', 'stb_count', 'last_modified'])

    def handle_pop_fmc_merge_package(self, memo: Memo) -> None:
        """
        Method to do processing post fmc merge package activation/deactivation

        :param emmo: Memo instance
        """
        change = memo.change
        pkg_code, (old_vals, new_vals) = list(
            change.get(  # type: ignore
                "packages"
            ).items()
        )[0]
        subscription = memo.instance
        if old_vals:
            self._handle_pop_fmc_merge_package_deactivation(memo)
            try:
                fmc_link = FmcSubscriptionLink.objects.get(
                    mobile_subscription=subscription, deactivated_at=None
                )
                fmc_link.deactivated_at = datetime_now()
                fmc_link.save()
            except FmcSubscriptionLink.DoesNotExist:
                raise
            subscription.fmc_merge_status = None
            subscription.save(update_fields=['fmc_merge_status'])
            subscription.addresses.remove()

    def update_subscription_cms_status(self, pkg: Package, sub: Subscription) -> None:
        """
        update the status of CMS order to closed.

        :param pkg: package instance
        :param sub: subscription instance
        """

        qs = SubscriptionCmsOrders.objects.filter(
            subscription=sub,
            package=pkg,
            end_date=None,
            request_type=FttbCMSClient.request_types['subscriber_trouble'],
        )
        if qs.exists():
            update_record = qs.latest('id')
            update_record.end_date = today()
            update_record.cms_order_status = CMS_CASE_CLOSED
            update_record.save()

    def handle_tv_service(
        self, package: Package, subscription: Subscription, new_vals: Optional[dict]
    ) -> None:
        """
        Method to hanlde tv services and subscription changes accordingly

        :param package: package instance
        :param subscription: Subscription instance
        :param new_vals: New attributes
        """
        if not package.has_service_in(
            [TVSERVICE_SERVICE, TVTUNER_SERVICE, ADDITIONALSCREEN_SERVICE]
        ):
            return
        if package.has_service(TVSERVICE_SERVICE):
            if new_vals is None:
                subscription.ap_count = None
                subscription.stb_count = None
            else:
                subscription.ap_count = DEFAULT_AP_COUNT
                subscription.stb_count = DEFAULT_STB_COUNT

        if (
            new_vals is None
            and subscription.ap_count is None
            and subscription.stb_count is None
        ):
            return

        if package.has_service(TVTUNER_SERVICE):
            sub_pkg = subscription.xref_packages.active(package=package).first()
            if sub_pkg and sub_pkg.recurrence_status in [
                RECURRENCE_STATUS_WAIT_FOR_ACTIVATION,
                RECURRENCE_STATUS_PRE_ACTIVATION,
            ]:
                return
            if new_vals is None:
                subscription.ap_count -= 1  # type: ignore
                subscription.stb_count -= 1  # type: ignore
            elif (
                subscription.ap_count is not None and subscription.stb_count is not None
            ):
                subscription.ap_count += 1  # type: ignore
                subscription.stb_count += 1  # type: ignore

        if package.has_service(ADDITIONALSCREEN_SERVICE):
            if new_vals is None:
                subscription.ap_count -= 1  # type: ignore
            else:
                subscription.ap_count += 1  # type: ignore

    @staticmethod
    def mark_first_refill_status(subscription: Subscription) -> None:
        """
        Marks first refill status done if first-refill tag is available in any of
        active packages for the first time

        :param subscription: Subscription
        :raises ValueError
        """
        with transaction.atomic():
            subscription.refresh_from_db()
            if not subscription.is_first_refill_done:
                active_packages = SubscriptionUtil.get_active_packages(
                    subscription, query_set=True
                )
                if isinstance(active_packages, list):
                    raise ValueError('Invalid queryset recieved')
                if active_packages.filter(
                    package__tags__code__in=[FIRST_REFILL_TAG]
                ).exists():
                    subscription.is_first_refill_done = True
                    subscription.save(
                        update_fields=['is_first_refill_done', 'last_modified']
                    )

    @staticmethod
    def get_wttx_preorder_event_details(subscription: Subscription) -> dict:
        """
        Gets wttx pre-order event details

        :param subscription: subscription instance
        :return: wttx pre-order event details
        """
        wttx_preorder_event_details = {
            "subscriberId": subscription.msisdn,
            "eventId": WTTX_PREORDER_EVENT_ID,
            "eventTime": parse_iso_date_to_localtime(datetime_now().isoformat()),
            "subscriberType": 1 if subscription.is_fttb_subscription else 0,
            "params": {
                "cosName": None,
                "time": None,
                "nextApplDt": None,
                "wttxConfirmed": "Y",
            },
        }
        return wttx_preorder_event_details

    def _deactivate_separate_account_ba(self, memo: Memo, package: Package) -> None:
        """
        Method to deactivate separate account BA.

        :param memo: Memo instance
        :param package: Package instance
        """
        if not package.has_tag(PACKAGE_TAG_SEPARATE_ACCOUNT):
            return
        subscription = memo.instance
        if subscription.separate_account_billing_account:
            ba = subscription.separate_account_billing_account
            ba_balance_util = BillingAccountBalanceUtil(ba)
            main_balance = ba_balance_util.rbs_main_balance
            changes = {
                'status': [ba.status, INACTIVE],
                "termination_balance": [ba.termination_balance, main_balance],
            }
            ba.request_update(
                user=memo.actor,
                changes=changes,
                charge={},
                handler='billing_account_status',
                scheduled_at=datetime_now(),
                newtlet="customers",
            )

    def do_next(self, memo: Memo):
        change = memo.change
        subscription = memo.instance
        delete_only = change.get("delete_only", False)
        if delete_only:
            return

        package_code, (old_val, new_val) = list(
            change.get(  # type: ignore
                "packages"
            ).items()
        )[0]

        if package_code in (PACKAGE_CODE_POPFMCADDON,):
            update_subscription_in_case_management(subscription)

        package = Package.objects.get(code=package_code)

        FriendsAndFamilyUtil.request_faf_update(memo)
        # the below implementation is for the dual sim packages, like internet
        # sim/spare sim etc,
        # where we need to stock out the second sim, when activated, from the
        # physical inventory
        package_code, (old_val, new_val) = memo.change["packages"].popitem()
        AdditionalNumberPackageUtil.update_inventory_details(
            subscription=subscription,
            package_code=package_code,
            old_val=old_val,
            new_val=new_val,
        )
        DirectNumberPackageUtil.update_inventory_details(memo)
        FAXORCDMAPackageUtil.update_inventory_details(memo)
        StaticIPServicePackageUtil.update_ip_address(memo)
        FaxToEmailUtil.update_msisdn_details(memo, package)
        stock_out_data = PackageUtil.stock_out_change_data(memo)
        if package.has_service(CORPORATEVPN_SERVICE):
            if old_val:
                apn = old_val[CORPORATEVPN_SERVICE][APN_ID_ATTRIBUTE]
                ip = old_val[CORPORATEVPN_SERVICE].get(STATIC_IP_ADDRESS_ATTRIBUTE)
                if ip:
                    Inventory.mark_apn_ip_combination(apn, ip, STATUS_QUARANTINE)
            if new_val:
                apn = new_val[CORPORATEVPN_SERVICE][APN_ID_ATTRIBUTE]
                ip = new_val[CORPORATEVPN_SERVICE].get(STATIC_IP_ADDRESS_ATTRIBUTE)
                if ip:
                    Inventory.mark_apn_ip_combination(apn, ip, STATUS_IN_USE)

        # skip stock out action if it is a starter kit order
        starter_kit = None
        if subscription.sim_card:
            starter_kit = Inventory.get_slave_details(subscription.sim_card.icc)
        if stock_out_data:
            # check for stockout data and if exists check for starter_kit icc
            # match with stock out data icc, ICC stockout
            # happens on ICC which is not related to starterkit order
            # but fresh sim change.
            if (
                starter_kit and starter_kit['icc'] != stock_out_data['icc'][1]
            ) or not starter_kit:
                subscription.request_update(
                    user=Memo.make_actor(subscription.user_id),
                    changes=stock_out_data,
                    charge={},
                    handler=StockOutChangeSimHandler.name,
                    scheduled_at=datetime_now(),
                )

        PackageUtil.create_sim_card(
            memo, package=package, old_val=old_val, new_val=new_val
        )
        PackageUtil.terminate_sim_card(
            package=package, old_val=old_val, new_val=new_val
        )
        SpareSimPackageUtil.sim_card_update(
            memo, package=package, old_val=old_val, new_val=new_val
        )
        PontisDiscountPackageUtil.create_pontis_discount(memo)
        if new_val is None:
            self._deactivate_separate_account_ba(memo, package)

        # Barrings to be applied/ removed based on package activation/deactivation
        self._request_barrings_update(memo)

        pkg_code, (old_vals, new_vals) = list(
            change.get(  # type: ignore
                "packages"
            ).items()
        )[0]

        if pkg_code == INTERNET_SIMPAIR_PACKAGE_CODE:
            if new_vals is not None:
                self.send_sms_notification(
                    memo, pkg_code, INTERNET_SIMPAIR_REQUESTCONFIRM
                )
            if old_vals is not None:
                self.send_sms_notification(
                    memo, pkg_code, INTERNET_SIMPAIR_DEACTIVATION
                )

        if (
            pkg_code in [FMCKYIVTVPACKIVI, FTTBKYIVTVPACKIVI]
            and old_vals is not None
            and memo.is_ongoing
        ):
            self.send_sms_notification(memo, pkg_code, POP_FTTB_TVPACK_IVI_DEACTIVATION)

        if package_code in WOT_PACKAGES and new_val is None:
            subscription.wot_eligibility_status = True
            subscription.save(update_fields=['wot_eligibility_status', 'last_modified'])

        pkg = Package.objects.get(code=pkg_code)

        if any([WEEK_OF_TRUST_IDENTIFIER in pkg_code, WEEK_OF_TRUST == pkg_code]):
            if all(
                [
                    (
                        subscription.is_fttb_subscription
                        or subscription.is_fixed_line_subscription
                    ),
                    pkg.has_tag(PACKAGE_TAG_ALLOWED_FOR_SUSPENSION),
                    subscription.has_barring(BARRING_FTTB_LIFECYCLE),
                ]
            ):
                if subscription.has_barring(BARRING_FTTB_LIFECYCLE):
                    PackageHandler.remove_barring(
                        memo, BARRING_FTTB_LIFECYCLE, BarringLevel.LEVEL_OPERATOR
                    )
            elif subscription.is_fmc_subscription:
                # Remove BAR_AAAVIASAT only on WOT activation.
                if subscription.has_barring(BAR_AAAVIASAT) and new_val is not None:
                    PackageHandler.remove_barring(
                        memo, BAR_AAAVIASAT, BarringLevel.LEVEL_OPERATOR
                    )
                elif all(
                    [
                        subscription.has_barring(BAR_AAA),
                        subscription.has_barring(BAR_VIASAT),
                    ]
                ):
                    PackageHandler.remove_barring(
                        memo, BAR_AAA, BarringLevel.LEVEL_OPERATOR
                    )
                    PackageHandler.remove_barring(
                        memo, BAR_VIASAT, BarringLevel.LEVEL_OPERATOR
                    )

        if pkg_code == CREDITTRANSFER_CODE and old_val is None and new_val is not None:
            self.send_sms_notification(
                memo, pkg_code, TEMPLATE_CREDITTRANSFER_ACTIVATION
            )

        if pkg_code in POP_FTTB_TVPACKS_CHOICES and memo.change['is_scheduled']:
            self.send_sms_notification(
                memo, pkg_code, TEMPLATE_POP_FTTB_TVPACK_ORDER_TO_CHANGE_COMPLETED
            )
        if pkg_code in [
            DIRNUM40PSTN10_CODE,
            DIRNUM0PSTN10_CODE,
            DIRNUM40PSTN30_CODE,
            DIRNUM0PSTN30_CODE,
        ]:
            template_parameters = dict()
            if new_vals:
                direct_number_changes = new_vals.get("DIRECTNUMBER")
                if direct_number_changes:
                    for key, value in direct_number_changes.items():
                        if key.endswith("NUMBER") and value:
                            template_parameters['pstn_number'] = value
                            template_id = (
                                POP_DIRECTNUMBER_APPLICATION_TO_CONNECT_OR_CHANGE_NUMBER
                            )
                            notification_sms_config(
                                template_id,
                                memo.actor,
                                subscription,
                                template_parameters,
                                create_memo=False,
                            )

        wttx_old_val = (
            old_vals.get(WTTX_SERVICE_CODE, {}).get(WTTX_CONFIRMED_ATTRIBUTE, '')
            if old_vals
            else ''
        )
        wttx_change_val = (
            new_vals.get(WTTX_SERVICE_CODE, {}).get(WTTX_CONFIRMED_ATTRIBUTE, '')
            if new_vals
            else ''
        )

        if all(
            [
                wttx_change_val.upper() == 'Y',
                wttx_old_val.upper() != wttx_change_val.upper(),
            ]
        ):
            wttx_preorder_event_details = PackageHandler.get_wttx_preorder_event_details(
                subscription
            )
            DMP.send_wttx_preorder_event_to_dmp(wttx_preorder_event_details)

        # deactivate service master
        if (
            new_vals
            and VISITSERVICE in new_vals
            and new_vals[VISITSERVICE].get(VISITCONFIRMED)
            == SERVICE_ATTRIBUTE_VALUE_CANCEL
        ):
            memo.instance.request_package_update(
                actor=memo.actor,
                changes={pkg_code: False},
                channel=memo.change.get('channel', settings.CRM_CHANNEL),
                package=package,
                scheduled_at=datetime_now(),
            )
        kwargs = change.pop("child_package_kwargs", {})
        kwargs.pop('scheduled_at', None)
        if package.has_tag(PACKAGE_TAG_VPBX_FIX_TRYBUY):
            kwargs["scheduled_at"] = morning(
                add_to_date(
                    days=VPBX_FIX_TRYBUY_SCHEDULED_DAYS,
                    base=kwargs.get("scheduled_at", memo.scheduled_at),
                )
            )
        else:
            kwargs["scheduled_at"] = datetime_now()
        if change.get("skip_childs", False) not in [True]:
            subscription.request_child_package_updates(memo.actor, change, **kwargs)
        if package.has_service(POPFMCMERGE_SERVICE):
            self.handle_pop_fmc_merge_package(memo)
        elif package.has_service(ALWAYSTOGETHER_SERVICE_CODE):
            if new_vals is not None:
                faf_id = (
                    new_vals.get(ALWAYSTOGETHER_SERVICE_CODE, {}).get(
                        FAF_ID_1_SERVICE_ATTRIBUTE, ''
                    )
                    if new_vals
                    else ''
                )
                template_params = {'faf_id': faf_id}
                notification_sms_config(
                    ALWAYS_TOGETHER_ACTIVATION,
                    memo.actor,
                    subscription,
                    template_params,
                )
        if package.code == SELF_SIM_CHANGE:
            hours = Option.get_remote_sim_network_service_activation_duration()
            memo.instance.request_update(
                user=Memo.make_actor(memo.instance.user_id),
                changes={},
                charge={},
                handler=GetVLRStatusHandler.name,
                scheduled_at=add_to_date(hours=hours),
                deferred=True,
            )

        if package.has_tag(PACKAGE_TAG_FORCE_STC):
            subscription.request_update(
                user=Memo.make_actor(memo.instance.user_id),
                changes=ForceSTCUtility.get_changes(subscription=subscription),
                charge={},
                handler=ForceSubscriptionTypeHandler.name,
                scheduled_at=datetime_now(),
                deferred=True,
            )
        if package.has_tag(PACKAGE_TAG_SEPARATE_ACCOUNT):
            update_subscription_in_case_management(subscription)

        if subscription.is_fixed_line_subscription and subscription.is_pilot_number:
            self._handle_fixedline_additional_number_packages(memo)

        if change.get('assign_current_subscription'):
            current_subscription = Subscription.objects.get(
                id=change['current_subscription_id']
            )
            option82_changes = {'option82': change.get('option82', {})}
            current_subscription.request_update(
                memo.actor,
                option82_changes,
                charge={},
                handler=Option82Handler.name,
                scheduled_at=datetime_now(),
                deferred=True,
            )

    def _handle_fixedline_additional_number_packages(self, memo):

        package_code, (old_val, new_val) = list(memo.change.get("packages").items())[0]
        if package_code not in PILOT_TO_ADDITIONAL_PKG_MAPPING:
            return

        for sub_rel in SubscriptionRelation.objects.filter(pilot_number=memo.instance):
            additional_no_subscription = sub_rel.child_number
            if additional_no_subscription.contract.status == Contract.TERMINATED:
                continue

            try:
                if new_val is None:
                    change = {PILOT_TO_ADDITIONAL_PKG_MAPPING[package_code]: False}
                else:
                    change = {PILOT_TO_ADDITIONAL_PKG_MAPPING[package_code]: new_val}

                barring_memo = additional_no_subscription.request_package_update(
                    actor="package_handler_do_next",
                    changes=change,
                    channel=settings.DEFAULT_CHANNEL,
                    scheduled_at=datetime_now(),
                )
                """
                Special case: FIXEDSERPAUSE was not migrated on additional number, only barring was migrated.

                Problem: FIXEDSERPAUSE deactivation is supposed to remove barring as part of deactivation process.
                         As package is not migrated on additional number, barring cannot be removed.

                Solution: Memo will be none when we will try to deactivated FIXEDSERPAUSE on additional number.
                          So, try to remove barring on additional number directly.
                """
                if barring_memo is None and new_val is None:
                    additional_no_subscription.request_update(
                        user="package_handler_do_next_mig_fix",
                        changes={
                            'barrings': {
                                'barring': BARRING_VOLUNTARY_SUSPENSION,
                                str(BarringLevel.LEVEL_CUSTOMER): False,
                            }
                        },
                        charge={},
                        handler='barrings',
                        scheduled_at=datetime_now(),
                        check_blocking_conditions=False,
                    )

            except BlockingConditionError as e:
                self.logger.exception(
                    "Error while activating package=%s on additional number %s, Error: %s",
                    package_code,
                    additional_no_subscription.msisdn,
                    str(e),
                )

    @staticmethod
    def _handle_pop_fmc_merge_package_deactivation(memo: Memo) -> None:
        """
        Method to handle the deactivation of fmc pop merge package

        :param memo: Memo instance
        """
        subscription = memo.instance
        change = memo.change
        package_code, (old_val, new_val) = list(
            change.get(  # type: ignore
                "packages"
            ).items()
        )[0]

        if new_val is None:
            package = Package.objects.get(code=package_code)
            if package.has_tag(PACKAGE_TAG_ROUTER_DEPENDENCY):
                router_package_to_deactivate = subscription.xref_packages.active(
                    package__tags__code=PACKAGE_TAG_ROUTERS_FMC_POP
                ).values_list('package__code', flat=True)
                if router_package_to_deactivate:
                    for pkg_code in router_package_to_deactivate:
                        subscription.request_package_update(
                            actor=memo.actor,
                            changes={pkg_code: False},
                            channel=settings.DEFAULT_CHANNEL,
                            scheduled_at=datetime_now(),
                        )
                if subscription.xref_packages.active(package__code='OPTION82'):
                    subscription.request_package_update(
                        actor=memo.actor,
                        changes={'OPTION82': False},
                        channel=settings.DEFAULT_CHANNEL,
                        extra_changes={"provisioning": False},
                        scheduled_at=datetime_now(),
                    )

    @staticmethod
    def remove_barring(memo: Memo, code: str, level: int) -> None:
        """
        removes barring

        param memo: memo instance
        param code: barring code
        param level: barring level
        """

        if not memo.instance.barrings.filter(barring__code=code).exists():
            return
        request_changes = [{'barring': code, str(level): False}]
        memo.instance.request_update(
            user=memo.actor,
            changes={'barrings': request_changes},
            charge={},
            handler='barrings',
            scheduled_at=datetime_now(),
            newtlet='services',
            check_blocking_conditions=False,
            parent_memo=memo,
        )

    @staticmethod
    def add_barring(memo: Memo, code: str, level: int) -> None:
        """
        Adds barring

        param memo: memo instance
        param code: barring code
        param level: barring level
        """

        if memo.instance.barrings.filter(barring__code=code).exists():
            return
        request_changes = [{'barring': code, str(level): True}]
        memo.instance.request_update(
            user=memo.actor,
            changes={'barrings': request_changes},
            charge={},
            handler='barrings',
            scheduled_at=datetime_now(),
            newtlet='services',
            check_blocking_conditions=False,
            parent_memo=memo,
        )

    @staticmethod
    def _request_barrings_update(memo: Memo) -> None:
        """
        Depending on package activation/deactivation:
        applies / removes the barrings with the barring level
        configured for the package in PC barring relation

        :param memo: Related Memo Instance
        """
        change = memo.change
        package_code, (old_val, new_val) = change["packages"].popitem()
        has_barring = None
        if new_val is not None and old_val is None:
            # package is added
            has_barring = True
        elif old_val is not None and new_val is None:
            # package is removed
            has_barring = False
        if has_barring is not None:
            barring_relations = BarringRelation.objects.filter(
                package__code=package_code
            )
            for barring_relation in barring_relations:
                for barring in barring_relation.barrings.all():
                    request_changes = [
                        {
                            'barring': barring.code,
                            str(barring_relation.barring_level.level): has_barring,
                        }
                    ]

                    changes = {'barrings': request_changes}
                    if package_code == FTTBSERPAUSE_PACKAGE_CODE:
                        if new_val is None:
                            reduction_percentage = old_val.get(
                                DISCSERPAUSE_SERVICE, {}
                            ).get(DISCOUNTPLANCODES_SERVICE_ATTRIBUTE)
                        else:
                            reduction_percentage = new_val.get(
                                DISCSERPAUSE_SERVICE, {}
                            ).get(DISCOUNTPLANCODES_SERVICE_ATTRIBUTE)
                        changes["reduction_percentage"] = reduction_percentage

                    memo.instance.request_update(
                        user=memo.actor,
                        changes=changes,
                        charge={},
                        handler='barrings',
                        scheduled_at=datetime_now(),
                        newtlet='services',
                        check_blocking_conditions=False,
                        parent_memo=memo,
                    )

    def do_fail(self, memo: Memo):
        """
        Package related action has failed. In case package was tried
        to be activated/updated the failure SMS should be sent to
        customer.

        :param memo: The related memo instance.
        """
        if memo.change.get('balance_check_failed'):
            # Notification is already sent for insufficient fund from validate_postpaid_rtcc_balance
            return
        pkg_code, (old_vals, new_vals) = memo.change["packages"].popitem()
        subscription = memo.instance
        package = OfferAdapterUtil.validate_and_get_package(
            package_code=pkg_code, action='setting', subscription_id=subscription.id
        )
        if old_vals is None:
            if pkg_code in NOTIFY_ME_LIST and new_vals:
                if new_vals.get(NOTIFYME_SERVICE, {}).get(FREE_LINE_ATTRIBUTE) == 'YES':
                    self.send_sms_notification(
                        memo, pkg_code, NOTIFY_ME_FREELINE_FAILURE
                    )
                elif (
                    new_vals.get(NOTIFYME_SERVICE, {}).get(DAY_MODE_ATTRIBUTE) == 'YES'
                ):
                    self.send_sms_notification(
                        memo, pkg_code, NOTIFY_ME_DAYMODE_FAILURE
                    )
            elif (
                package
                and package.product_type == str(PRODUCT_TYPE_11)
                and memo.flow_data
            ):
                memo_failure_reason = memo.flow_data.last().data.get('reason')
                if (
                    pkg_code in POP_FTTB_TVPACKS_CHOICES
                    and not memo.instance.is_first_refill_done
                ):
                    template_id = (
                        TEMPLATE_POP_FTTB_TVPACK_ACTIVATION_FAIL_NO_FIRST_PAYMENT
                    )
                elif memo_failure_reason and (
                    INSUFFICIENT_BALANCE_FAILURE_REASON in memo_failure_reason
                    or OFFER_PRICE_EXCEEDED_FAILURE_REASON in memo_failure_reason
                ):
                    template_id = (
                        SubscriptionPackageEligibilityUtil.INSUFFICIENT_BALANCE_TO_ACTIVATE_TEMPLATE
                    )
                else:
                    template_id = "activation_failure_template_id"
                self.send_sms_notification(memo, pkg_code, template_id)
            else:
                self.send_sms_notification(
                    memo, pkg_code, "activation_failure_template_id"
                )
        elif old_vals and new_vals:
            if pkg_code == INT_SIMPAIR_POS_PACKAGE_CODE:
                self.send_sms_notification(
                    memo, pkg_code, INTERNET_SIMPAIR_PREINSTALL_FAIL
                )
            elif pkg_code in NOTIFY_ME_LIST:
                self.send_sms_notification(memo, pkg_code, NOTIFY_ME_MODIFY_FAILURE)
            elif pkg_code == ADDITIONAL_NUMBER_CODE:
                self.send_sms_notification(
                    memo, pkg_code, ADDITIONAL_NUMBER_CHANGE_FAIL
                )
            elif pkg_code in [
                ADDITIONAL_NUMBER_POST_CODE,
                ADDITIONAL_NUMBER_12UAH_POST_CODE,
                ADDITIONAL_NUMBER_30UAH_POST_CODE,
                ADDITIONAL_NUMBER_5UAH_POST_CODE,
                ADDITIONAL_NUMBER_50UAH_POST_CODE,
            ]:
                if AdditionalNumberPackageUtil.get_virtual_msisdn(new_vals, old_vals):
                    self.send_sms_notification(
                        memo, pkg_code, POP_ADDITIONAL_NUMBER_CHANGE_FAIL
                    )
                elif AdditionalNumberPackageUtil.get_temporary_suspension(
                    new_vals, old_vals
                ):
                    self.send_sms_notification(
                        memo, pkg_code, POP_ADDITIONAL_NUMBER_SUSPENSION_FAILURE
                    )
                elif AdditionalNumberPackageUtil.get_temporary_suspension_reactivation(
                    new_vals, old_vals
                ):
                    self.send_sms_notification(
                        memo,
                        pkg_code,
                        POP_ADDITIONAL_NUMBER_REACTIVATION_AFTER_SUSPENSION_FAILURE,
                    )
            elif pkg_code in [
                DIRNUM40PSTN10_CODE,
                DIRNUM0PSTN10_CODE,
                DIRNUM40PSTN30_CODE,
                DIRNUM0PSTN30_CODE,
            ]:
                self.send_sms_notification(
                    memo, pkg_code, POP_DIRECTNUMBER_REQUEST_TO_CHANGENUM_FAIL_TECHISSUE
                )
            elif pkg_code in POP_FTTB_TVPACKS_CHOICES and memo.change['is_scheduled']:
                self.send_sms_notification(
                    memo, pkg_code, TEMPLATE_POP_FTTB_TVPACK_ORDER_TO_CHANGE_FAIL
                )
            elif (
                pkg_code in POP_FTTB_TVPACKS_CHOICES
                and subscription.contract.status != Contract.ACTIVE
            ):
                self.send_sms_notification(
                    memo, pkg_code, TEMPLATE_POP_FTTB_TVPACK_ORDER_TO_CHANGE_NO_INTERNET
                )
            elif (
                pkg_code in [FMCKYIVTVPACKIVI, FTTBKYIVTVPACKIVI]
                and subscription.contract.status != Contract.ACTIVE
            ):
                self.send_sms_notification(
                    memo, pkg_code, POP_FTTB_TVPACK_IVI_DEACTIVATION_FAIL_UNKNOWN_ERROR
                )
            elif package.has_tag(PACKAGE_TAG_FAX_TO_EMAIL):
                self.send_sms_notification(
                    memo, pkg_code, FAXTOEMAIL_CHANGE_SERVICE_FAILURE
                )

        else:
            if (
                pkg_code in NOTIFY_ME_LIST
                and new_vals.get(NOTIFYME_SERVICE, {}).get(FREE_LINE_ATTRIBUTE) == 'YES'
            ):
                self.send_sms_notification(
                    memo, pkg_code, NOTIFY_ME_FREELINE_DEACTIVATE_FAILURE
                )
            self.send_sms_notification(
                memo, pkg_code, "deactivation_failure_template_id"
            )

        if pkg_code == PROMO5000MB_CODE and pkg_code in subscription.included_packages:
            self.send_sms_notification(
                memo, pkg_code, POP_PROMOPACKXMB_FAILURE_RECONNECTING
            )

    @staticmethod
    def on_created(memo: Memo):
        change = memo.change
        subscription: cm.Subscription = memo.instance
        process_name = None
        if change and change.get("packages"):
            pkg_code, (old_vals, new_vals) = change["packages"].popitem()
            if pkg_code in POP_FTTB_TVPACKS_CHOICES and not memo._is_scheduled_now():
                process_name = POP_FTTB_TVPACKS_ORDER
            elif (
                pkg_code in FMC_HOMETV_TVPACKS_CHOICES and not memo._is_scheduled_now()
            ):
                process_name = FMC_HOMETV_TVPACKS_ID
            elif (
                pkg_code in [FMCKYIVTVPACKIVI, FTTBKYIVTVPACKIVI]
                and old_vals is not None
                and memo.is_ongoing
            ):
                process_name = POP_FTTB_TVPACK_IVI_DEACTIVATION
            if process_name:
                notification_sms_config(
                    process_name, memo.actor, subscription, create_memo=False
                )

    @staticmethod
    def invalidate_cache(pkg_id, subscription_id):
        """Invalidate package form cache for updated service values.

        :param int pkg_id: Package id
        :param int subscription_id: Subscription id
        """
        cache_key = cm.SUB_PKG_CACHE_KEY.format(subscription_id, pkg_id)
        cached = cache.get(cache_key, None)
        if cached:
            cache.delete(cache_key)

    @staticmethod
    def synchronize_multi_surf(instance, old, new):
        """Synchronize multi surf subscriptions as new contracts to RBS"""
        from customers.contracts.rpc.serializers import MultiSurfContractRBSSync

        new_values = new.get('MULTI_SURF', {})
        old_values = old.get('MULTI_SURF', {})
        activate_list = [
            key[-1]
            for (key, val) in new_values.items()
            if 'msisdn' in key and not old_values.get(key) == val
        ]

        ctr = instance.contract
        for ms in activate_list:
            ctr.ms_subscription = {
                "msisdn": new_values["multi_surf_msisdn" + ms],
                "icc": new_values.get("multi_surf_icc" + ms),
                "imsi": new_values.get("multi_surf_imsi" + ms),
            }
            MultiSurfContractRBSSync().send(ctr, None, None, None, None)

    @staticmethod
    def on_send(memo):
        change = memo.change
        subscription: cm.Subscription = memo.instance
        if change and change.get("packages"):
            pkg_code, (old_vals, new_vals) = change.get("packages").popitem()
            if pkg_code == INTERNET_SIMPAIR_PACKAGE_CODE:
                PackageHandler.send_sms_notification(
                    memo, pkg_code, INTERNET_SIMPAIR_SELF_ACTIVATE
                )
            elif pkg_code == INT_SIMPAIR_EXTRA_PACKAGE_CODE:
                PackageHandler.send_sms_notification(
                    memo, pkg_code, INTERNET_SIMPAIR_PREINSTALL_ACTIVE
                )

    def handle_pending(self, memo: Memo):
        change = memo.change
        if change and change.get("packages"):
            pkg_code, (old_vals, new_vals) = memo.change["packages"].popitem()
        self.send_sms_notification(
            memo,
            pkg_code,
            SubscriptionPackageEligibilityUtil.INSUFFICIENT_BALANCE_TO_ACTIVATE_TEMPLATE,
        )


class PackageProvisioningHandler(PackageHandler):
    model = cm.Subscription
    name = 'services_provisioning'
    newtlet = 'services'

    def serialize_for_task(self, memo):
        """
        Serialize only the package provisioning activation request task
        data.

        :param Memo memo: The related memo instance.
        :return: Serialized data.
        :rtype: dict.
        """
        change = memo.change
        subscription: cm.Subscription = memo.instance
        package_code, (old_vals, new_vals) = change.get("packages").popitem()
        send_services = change.get("send_services", True)
        provisioning = change.get('provisioning', True)

        task_builder = TaskBuilder(instance=subscription)
        prov_node = task_builder.add_node()
        package = OfferAdapterUtil.validate_and_get_package(
            package_code=package_code, action='setting', subscription_id=subscription.id
        )

        # Eligibility check for package activation when the subscription
        # is in the suspended status.
        if new_vals is not None:
            if all(
                [
                    not subscription.validate_package_allowed_for_suspended(
                        package_code
                    ),
                    not package.has_tag(PACKAGE_TAG_UNSUSPEND_BEFORE_ACTIVATION),
                ]
            ):
                raise TaskAdapterSerializingException(
                    f"Package {package_code} can not be activated as the subscription "
                    f"is in suspended status."
                )
        # if paid is True, from requested to active, request provisioning
        if change['paid']:
            # check package, keeping same logic as regular package activation
            self.validate_package_cardinality(subscription, package)
            self.validate_package_eligibility(subscription, package)
            self.validate_package_cardinality_variant(subscription, package)
            change.update({"activation_type": PTARegistry.PACKAGE_ACTIVATION})
            PackageTaskAdapterAdder._update_package_offer_info(change, subscription)

            # get provisioning adapters, and add corresponding task builders
            prov_adapters = copy.deepcopy(
                PTARegistry.get_task_adapters(
                    package, PTARegistry.REGISTRY_KEY_SET_PROV
                )
            )
            if prov_adapters:
                # provisioning adapters

                extra = None
                if package.has_service_in([TVSERVICE_SERVICE, NOCHANGE_TVSERVICE]):
                    # list(dict.fromkeys) used instead of set only to preserve order for test cases
                    all_profiles = ",".join(
                        list(
                            dict.fromkeys(
                                subscription.active_tv_profiles
                                + [package.service_profile_name]
                            )
                        )
                    )
                    ap_count = subscription.ap_count or DEFAULT_AP_COUNT
                    stb_count = subscription.stb_count or DEFAULT_STB_COUNT

                    stb_count, ap_count = PackageTaskAdapterAdder.get_stb_and_ap_count(
                        stb_count, ap_count, package
                    )

                    extra = {
                        "stb_count": stb_count,
                        "ap_count": ap_count,
                        "all_profiles": all_profiles,
                    }
                services = serialize_services(
                    services=old_vals,
                    memo=memo,
                    package_code=package_code,
                    is_deactivate_package=True,
                    send_services=send_services,
                    extra=extra,
                )
                for adapter in prov_adapters:
                    if provisioning:
                        prov_node.add_task(
                            task_class=adapter,
                            instance=subscription,
                            change=(package, services, change),  # type:ignore
                        )
        else:
            package = OfferAdapterUtil.validate_and_get_package(
                package_code=package_code,
                action='removing',
                subscription_id=subscription.id,
            )
            prov_adapters = copy.deepcopy(
                PTARegistry.get_task_adapters(
                    package, PTARegistry.REGISTRY_KEY_REMOVE_PROV
                )
            )
            if prov_adapters:
                # provisioning adapters
                services = serialize_services(
                    services=old_vals,
                    memo=memo,
                    package_code=package_code,
                    is_deactivate_package=True,
                    send_services=send_services,
                )
                for adapter in prov_adapters:
                    if provisioning:
                        prov_node.add_task(
                            task_class=adapter,
                            instance=subscription,
                            change=(package, services, change),  # type:ignore
                        )
        return {'tasks': task_builder.serialize(), 'msisdn': memo.instance.msisdn}

    def do_persist(self, memo):
        """ If memo.change has paid and is set to True,
        SubscriptionPackage recurrence status needs
        to be set to RECURRENCE_STATUS_ACTIVE
        else, keep status to RECURRENCE_STATUS_WAIT_FOR_ACTIVATION

        :param Memo memo: The related memo instance.
        """
        change = memo.change
        subscription = memo.instance
        package_code, (old_vals, new_vals) = change.get('packages').popitem()
        package_key = 'pk' if package_code.isdigit() else 'code'
        package = Package.objects.get(Q(**{package_key: package_code}))
        subscription.xref_packages.update_recurrence_status(package, change.get('paid'))


class ProductFamilyPackageChangeHandler(UpdateHandler):
    model = cm.Subscription
    name = 'product_family_package_change'
    newtlet = 'services'
    blocking_conditions = [
        'terminated',
        'sim_card_request_ongoing',
        'msisdn_request_ongoing',
        'suspended',
        'billing_account_cl_suspended',
        'termination_request_ongoing',
        'termination_scheduled_request_ongoing',
        'subscription_reactivation_request_ongoing',
    ]

    def get_change_display(self, change):
        pkg, (old, new) = change['packages'].popitem()
        service = pc.ProductFamily.objects.get(package__code=old.keys()[0])
        if change.get('reason'):
            return {
                pkg: [old.keys()[0], new.keys()[0]],
                'Service': service.name,
                'reason': change.get('reason'),
            }
        return {pkg: [old.keys()[0], new.keys()[0]], 'Service': service.name}

    def serialize_for_task(self, memo):
        """
        Serialize the data for product family package change tasks.

        :param Memo memo: The related memo.
        :return: Serialized data.
        :rtype: dict.
        :raises: TaskAdapterSerializingException
        """
        subscription: Subscription = memo.instance
        change = memo.change
        old_packages, new_packages = change.get('packages')
        old_pkg, old_vals = old_packages.popitem()
        new_pkg, new_vals = new_packages.popitem()
        task_builder = TaskBuilder(change, subscription)
        package_key = 'pk' if new_pkg.isdigit() else 'code'
        package = Package.objects.get(Q(**{package_key: new_pkg}))

        if old_pkg == new_pkg:
            if memo.change.get('re_activated'):  # package re-activation
                PackageHandler.validate_package_cardinality(subscription, package)
                # activation eligibility check
                if not (
                    subscription.is_fttb_subscription and subscription.contract.is_idle
                ):
                    self.check_package_activation_balance_eligibility(memo, package)
                # package re-activation tasks
                act_node = task_builder.add_node()
                rbs_node = task_builder.add_node(parent_node=act_node)
                sms_node = task_builder.add_node(parent_node=act_node)
                PackageTaskAdapterAdder.append_package_activation_tasks(
                    prov_node=act_node,
                    rbs_node=rbs_node,
                    other_node=sms_node,
                    memo=memo,
                    new_packages={new_pkg: new_vals},
                    activation_type=PTARegistry.PACKAGE_REACTIVATION,
                    logger=self.logger,
                )
            else:  # package service values update
                # package activation (service value update) tasks
                act_node = task_builder.add_node()
                rbs_node = task_builder.add_node(parent_node=act_node)
                sms_node = task_builder.add_node(parent_node=act_node)
                PackageTaskAdapterAdder.append_package_activation_tasks(
                    prov_node=act_node,
                    rbs_node=rbs_node,
                    other_node=sms_node,
                    memo=memo,
                    new_packages={new_pkg: new_vals},
                    activation_type=PTARegistry.PACKAGE_SERVICE_UPDATE,
                    logger=self.logger,
                    service_value_update=True,
                )

        else:  # product family package change
            # activation eligibility check task
            PackageHandler.validate_package_cardinality(subscription, package)
            if not (
                subscription.is_fttb_subscription and subscription.contract.is_idle
            ):
                self.check_package_activation_balance_eligibility(memo, package)
            # package removal tasks
            rem_node = task_builder.add_node()
            rem_sms_node = task_builder.add_node(parent_node=rem_node)
            PackageTaskAdapterAdder.append_package_removing_tasks(
                prov_node=rem_node,
                rbs_node=rem_node,
                other_node=rem_sms_node,
                memo=memo,
                old_packages={old_pkg: old_vals},
                logger=self.logger,
                new_pkg=package,
            )
            # package activation tasks
            act_node = task_builder.add_node(parent_node=rem_node)
            rbs_node = task_builder.add_node(parent_node=act_node)
            act_sms_node = task_builder.add_node(parent_node=act_node)
            if package.has_tag_in([PACKAGE_TAG_RBT, PACKAGE_TAG_CHANGE_DISCOUNT]):
                activation_type = PTARegistry.PACKAGE_SERVICE_UPDATE
            else:
                activation_type = PTARegistry.PACKAGE_ACTIVATION
            PackageTaskAdapterAdder.append_package_activation_tasks(
                prov_node=act_node,
                rbs_node=rbs_node,
                other_node=act_sms_node,
                memo=memo,
                new_packages={new_pkg: new_vals},
                activation_type=activation_type,
                logger=self.logger,
            )

        return {'tasks': task_builder.serialize(), 'msisdn': subscription.msisdn}

    def check_package_activation_balance_eligibility(
        self, memo: Memo, package: Package
    ):
        """
        Checks whether there is enough balance to activate the package.
        If there are no enough funds then it raises an exception and sends a notification for
        insufficient funds.

        :param memo: Memo
        :param package: Package
        :raises SubscriptionPackageEligibilityException
        """
        subscription: Subscription = memo.instance
        if subscription.is_postpaid:
            if (
                subscription.non_terminated_billing_account
                and subscription.non_terminated_billing_account.type
                and subscription.non_terminated_billing_account.type.rbs_cs_provisionable
            ):
                try:
                    ba_balance_util = BillingAccountBalanceUtil(
                        subscription.non_terminated_billing_account
                    )
                    balance = ba_balance_util.shared_da_balance
                except CsGeneralError as error:
                    raise ValidationError(error.message)
            else:
                return True
        else:
            try:
                balance = PrepaidUtil.available_balance(
                    memo.instance.msisdn, memo.instance.is_not_mobile_subscription
                )
            except HttpErrorResponse as error:
                raise ValidationError(error)
        pkg_code = package.code
        if package.fee_item and package.fee_item.price > balance:
            PackageHandler.send_sms_notification(
                memo,
                pkg_code,
                SubscriptionPackageEligibilityUtil.INSUFFICIENT_BALANCE_TO_ACTIVATE_TEMPLATE,
            )
            message = "Package activation failed due to insufficient funds"
            raise SubscriptionPackageEligibilityException(
                message, code=BaseApiExceptionCodes.BALANCE_INSUFFICIENT
            )

    def do_persist(self, memo):
        """Store the package/service statuses in DB after the requests are
        performed.

        :param Memo memo: The related memo instance.
        """
        change = memo.change
        old_pkgs, new_pkgs = change.get('packages')
        psa = PackageServiceAdapter(
            old_pkgs=old_pkgs,
            new_pkgs=new_pkgs,
            re_activated=change.get('re_activated', False),
        )
        for pkg, servs in psa.pkgs_to_remove.items():
            self.logger.debug(
                "Deactivating package %s with services: %s", pkg, str(servs.items())
            )
            util = PersistPackageChangeUtil(
                sub=memo.instance,
                change={"packages": {pkg: (servs, None)}},
                changed_at=memo.executed_at,
                memo=memo,
            )
            util.persist()
        for pkg, servs in psa.pkgs_to_add.items():
            self.logger.debug(
                "Activating package %s with services: %s", pkg, str(servs.items())
            )
            util_change = {"packages": {pkg: (None, servs)}}
            if change.get('re_activated'):
                util_change['re_activated'] = True
            util = PersistPackageChangeUtil(
                sub=memo.instance,
                change=util_change,
                changed_at=memo.executed_at,
                memo=memo,
            )
            util.persist()
        for pkg, values in psa.pkgs_with_changed_service_values.items():
            self.logger.debug("Updating package %s service values: %s", pkg, values)
            util = PersistPackageChangeUtil(
                sub=memo.instance,
                change={"packages": {pkg: values}},
                changed_at=memo.executed_at,
                memo=memo,
            )
            util.persist()

    def do_next(self, memo: Memo):
        change = memo.change
        sub: Subscription = memo.instance
        old_pkgs, new_pkgs = change.get('packages')  # type: ignore
        psa = PackageServiceAdapter(
            old_pkgs=old_pkgs,
            new_pkgs=new_pkgs,
            re_activated=change.get('re_activated', False),
        )

        old_child_packages: list = []
        new_child_packages: list = []
        for pkg, services in psa.pkgs_to_remove.items():
            if PackageRelation.objects.filter(
                parent_package__code=pkg,
                relation_type=PACKAGE_RELATION_AVAILABLE_WITH_EXCEPTIONS,
            ).exists():
                old_child_packages += ChildPackageUtil.get_available_child_with_exceptions(
                    package_codes=[pkg], sub_type=sub.type, checked_package_codes=None
                )
                old_child_packages += ChildPackageUtil.child_packages(
                    package_codes=[pkg], sub_type=sub.type, parent_deactivation=True
                )
            else:
                old_childs = ChildPackageUtil.child_packages(
                    package_codes=[pkg], sub_type=sub.type, checked_package_codes=None
                )
                for pkg in old_childs:
                    sub.request_package_update(
                        actor=memo.actor,
                        changes={pkg: False},
                        channel=memo.change.get('channel', settings.CRM_CHANNEL),
                    )

        for pkg, services in psa.pkgs_to_add.items():
            if PackageRelation.objects.filter(
                parent_package__code=pkg,
                relation_type=PACKAGE_RELATION_AVAILABLE_WITH_EXCEPTIONS,
            ).exists():

                new_child_packages += ChildPackageUtil.get_available_child_with_exceptions(
                    package_codes=[pkg], sub_type=sub.type, checked_package_codes=None
                )
            else:
                new_childs = ChildPackageUtil.child_packages(
                    package_codes=[pkg], sub_type=sub.type, checked_package_codes=None
                )
                for pkg in new_childs:
                    try:
                        child_package = Package.objects.get(code=pkg)
                        PackageHandler.validate_package_cardinality(sub, child_package)
                        PackageHandler.validate_can_activate_child_tv_pack(
                            sub, child_package
                        )
                        PackageHandler.validate_child_sb_package(sub, child_package)
                    except ValidationError:
                        continue
                    else:
                        sub.request_package_update(
                            actor=memo.actor,
                            changes={pkg: True},
                            channel=memo.change.get('channel', settings.CRM_CHANNEL),
                        )
        # Updating Inventory for IP address
        StaticIPServicePackageUtil.update_product_family_ip_address(memo)
        set_old_child_pkgs = set(old_child_packages)
        set_new_child_pkgs = set(new_child_packages)
        diff = set_old_child_pkgs - set_new_child_pkgs
        pcrs = PackageConflictingRules.objects.filter(
            conflict_type__in=[CONFLICT_TYPE_PREVENT, CONFLICT_TYPE_SKIP]
        )
        for pkg in diff:
            conflict_packages = [
                rules[0]
                for rules in pcrs.filter(package__code=pkg).values_list(
                    'conflict_packages'
                )
            ]
            subscription_active_package = sub.xref_packages.active(  # type: ignore
                package__in=conflict_packages
            ).first()
            if subscription_active_package is None:
                conflict_barrings = [
                    rules[0]
                    for rules in pcrs.filter(package__code=pkg).values_list(
                        'conflict_barrings'
                    )
                ]
                subscription_active_barring = sub.barrings.filter(  # type: ignore
                    barring__in=conflict_barrings
                ).first()
                if subscription_active_barring is None:
                    sub.request_package_update(
                        actor=memo.actor,
                        changes={pkg: False},
                        channel=memo.change.get('channel', settings.CRM_CHANNEL),
                    )

    def do_fail(self, memo: Memo):
        """
        When memo execution fails, send failure SMS.

        :param memo: The related memo instance.
        """
        change = memo.change
        old_packages, new_packages = change.get('packages')  # type: ignore
        new_pkg, new_vals = new_packages.popitem()
        PackageHandler.send_sms_notification(
            memo, new_pkg, DEFAULT_ACTIVATION_FAILURE_TEMPLATE_ID
        )


class OnchangePackageScheduledForm(FourpaneForm):

    memo: Memo
    is_ongoing = True
    template = 'admin/contracts/customer/scheduled_form.html'

    def __init__(self, data=None, instance=None, **kwargs):
        super(OnchangePackageScheduledForm, self).__init__(
            data=data, instance=instance, **kwargs
        )
        self.fp_action.cancel_scheduled_button = l_('Cancel request')
        self.fp_action.schedule_now_button = l_('Schedule now')

    def render(self, request):
        context = {'form': self, 'action': self.fp_action}
        context.update(csrf(request))
        return render_to_string(self.template, context)

    def save(self, request, **kwargs):
        cancel_scheduled_action = request.GET.get('cancel_scheduled_action')
        schedule_now_action = request.GET.get('schedule_now_action')
        if cancel_scheduled_action:
            self.cancel_request(request.user, self.memo)
        elif schedule_now_action:
            self.memo.scheduled_at = datetime_now()
            self.memo.send()
        return self.memo

    def cancel_request(self, user, memo: Memo):
        memo.cancel(user)
        packages = memo.change.get("packages")
        if packages is None:
            return
        if memo.handler.name in ['product_family_package_change']:
            package_code = list(packages[1].keys())[0]
        else:
            package_code = list(packages.keys())[0]
        package = task_adapters.OfferAdapterUtil.validate_and_get_package(
            package_code=package_code,
            action="setting",
            subscription_id=memo.instance.id,
        )
        NotificationUtil.notify_subscription_cannot_activate_package(
            subscription=memo.instance,
            package=package,
            actor=user,
            template_id=NotificationUtil.SCHEDULED_PACKAGE_ACTIVATION_WAS_CANCELED,
        )


def get_multiplier(package):
    """
    get multiplier object for package
    """
    return Multiplier.objects.filter(
        tag__in=[tag.id for tag in package.tags.all()]
    ).first()


class BasePackageForm(
    WaitForConfirmationFormMixin,
    ReasonSelectFormMixin,
    MultiplierPackageFormMixin,
    FourpaneForm,
):
    package: Package
    validators = None
    cleaners = None
    template = 'fourpane/package_edit_form.html'
    error_messages = {
        'required': _('This field is required.'),
        'invalid': _('Enter a valid value.'),
    }
    is_included = False
    required_css_class = 'required'
    scripts = [
        'js/schedule_change.js',
        'js/child_package_warning.js',
        'js/additional_number.js',
        'js/direct_number_service.js',
        'js/low_balance_sms.js',
    ]

    def __init__(self, *args, **kwargs):
        super(BasePackageForm, self).__init__(*args, **kwargs)
        multiplier = get_multiplier(self.package)
        is_package_activated = self.instance.xref_packages.active(
            package__code=self.package.code
        )
        if self.instance.is_fixed_line_subscription and multiplier:
            multiplier_value = self.instance.get_multiplier_value(self.package)
            self.fields["multiplier"] = forms.CharField(
                widget=forms.TextInput(attrs={'type': 'number'})
            )
            self.fields["multiplier"].initial = int(multiplier.multiplier_value)
            if multiplier_value:
                self.fields["multiplier"].initial = multiplier_value
            if is_package_activated:
                self.fields["multiplier"].widget.attrs['readonly'] = True
        if self.instance.has_service_from_package(self.package):
            self.errors['is_enabled'] = [
                _("Service contained in this package is already activated\n")
            ]
            for field_name, field in self.fields.items():
                field.widget.attrs['disabled'] = True
        self.is_included = self.instance.type.included_packages.filter(
            code=self.package.code
        ).exists()
        # Django orders fields based on field.creation_counter. Since
        # service attributes are now fetched from cache, their
        # creation_counter is less than counter of 'is_enabled' field.
        # Thus overriding the self.fields to keep 'is_enabled' always first.
        new_fields = OrderedDict()
        is_enabled = self.fields.pop('is_enabled')
        new_fields['is_enabled'] = is_enabled
        new_fields.update(self.fields)
        self.fields = new_fields
        self.initial.update(
            PackageUtil.get_initial_info(self.package, self, is_package_activated)
        )
        active_services = SubscriptionUtil.get_active_services(self.instance)
        if self.fp_action and self.fp_action.tab.allow_additional_params(self.package):
            self.fp_action.tab.alter_additional_params(
                self.fp_action.package, self.fields
            )
            for package_service in self.package.services.all():
                service = package_service.service
                for active_service in active_services:
                    if active_service.code == service.code:
                        attributes = SubscriptionUtil.get_active_service_attributes_of_service(
                            subscription=self.instance, service=active_service
                        )
                        for key, value in attributes.items():
                            self.fields.get(
                                '{}_{}'.format(service.code, key)
                            ).initial = value
        self.fields = AdditionalNumberPackageUtil.fields_update(
            self.instance, self.package, self.fields, active_services
        )
        self.fields = FAXORCDMAPackageUtil.fields_update(
            self.instance, self.package, self.fields, active_services
        )
        fields, fields_order = DirectNumberUtil.fields_update(
            self.instance, self.package, self.fields
        )

        if self.package.code in INTERNETSIMPAIR:
            self.fields['is_scheduled'].widget = forms.HiddenInput()

        if self.package.has_tag_in(
            [PACKAGE_TAG_FTTB_CMSANDCS, PACKAGE_TAG_FTTB_CMSANDCS_ACTIVATION]
        ):
            fields = FTTBUtil.fields_update(self.fields, self.fp_action.user)

        self.fields = fields
        for field in self.fields:
            if (
                STATIC_IP_FIELD.lower() in field.lower()
                or STATIC_IP_SESSION_TO_FIELD.lower() in field.lower()
            ):
                self.fields[field].widget = forms.HiddenInput()
                self.fields[field].widget.required = False
                self.fields[field].required = False
                self.fields[field].widget.readonly = 'readonly'
        self.order_fields(fields_order)

    def validate_cms_id_required_cases(
        self,
        package_eligibility_util: SubscriptionPackageEligibilityUtil,
        services: dict,
    ):
        """
        validates the CMS ID for master service from no -> yes, cancel
        and for routers/tuners from waitingforactivate -> activate

        :param package_eligibility_util: SubscriptionPackageEligibilityUtil instace
        :param services: services changes
        """

        if MasterServiceUtil.is_master_service_cancel(self.package, services):
            cms_id = services.get(VISITSERVICE, {}).get(SERVICE_ATTRIBUTE_CMS_ID)
            if cms_id:
                package_eligibility_util.validate_cms_id(cms_id)
            else:
                raise ValidationError(
                    l_('Please provice a CMS ID for cancelling master service.')
                )
        else:
            package_eligibility_util.validate_cms_id_on_attribute_change(services)

    def clean(self):
        package = self.fp_action.package
        if not self.data.get(STATIC_IP_FIELD):
            static_ip_data = PackageUtil.get_static_ip_address(self, package)
            self.cleaned_data.update(static_ip_data)
            self.initial.update(static_ip_data)
        if self.validators:
            for validator in self.validators:
                validator(self)
        if self.cleaners:
            for cleaner in self.cleaners:
                cleaner.clean(self)

        if self.fp_action.tab.allow_additional_params(self.package):
            # if form has other allowed parameters, validate additional
            # parameters
            try:
                self.fp_action.tab.validate_additional_params(
                    self.package, self.cleaned_data
                )
            except ValidationError as err:
                raise ValidationError(err)

        package_eligibility_util = SubscriptionPackageEligibilityUtil(
            subscription=self.instance,
            package=self.package,
            actor=self.fp_action.tab.request.user.username,
            channel=settings.DEFAULT_CHANNEL,
        )
        if self.cleaned_data['scheduled_date']:
            schedule_date = self.cleaned_data['scheduled_date']
            max_allowed_date = add_to_date(
                years=PACKAGE_MAX_CHANGE_SCHEDULABLE_YEARS
            ).date()
            if schedule_date > max_allowed_date:
                raise ValidationError(
                    l_(
                        'Scheduled Date should not be more than %(year)s years from current date.'
                    )
                    % {'year': PACKAGE_MAX_CHANGE_SCHEDULABLE_YEARS}
                )
        if self.cleaned_data['is_enabled']:
            skip_balance_check = (
                self.cleaned_data.get("DEVICE_Status") == STATE_WAITING_FOR_ACTIVATE
                or self.cleaned_data.get("is_scheduled") == SCHEDULED_FOR_SPECIFIC_DATE
            )
            package_eligibility_util.validate_package_activation(
                skip_balance_check=skip_balance_check
            )

            if self.package.has_tag_in(
                [PACKAGE_TAG_FTTB_CMSANDCS, PACKAGE_TAG_FTTB_CMSANDCS_ACTIVATION]
            ):
                cleaned_data = copy.deepcopy(self.cleaned_data)
                services = self.get_changes(cleaned_data)
                self.validate_cms_id_required_cases(
                    package_eligibility_util, services[self.package.code]
                )

            if self.package.has_service(ADDITIONALNUMBER_SERVICE):
                cleaned_data = copy.deepcopy(self.cleaned_data)
                services = self.get_changes(cleaned_data)
                package_eligibility_util.validate_additional_number_msisdn(
                    services[self.package.code]
                )
        else:
            package_eligibility_util.validate_package_deactivation()

        if self.check_pending_order(package_eligibility_util):
            self.cleaned_data["pending_order"] = True
        return super(BasePackageForm, self).clean()

    def check_pending_order(
        self, subscription_util_obj: SubscriptionPackageEligibilityUtil
    ) -> bool:
        """
        Method to validate whether the order is pending.

        :return: True if order is pending else False
        """

        if not subscription_util_obj.is_pending_order_possible:
            return False
        try:
            balance = subscription_util_obj.account_balance
            if not balance:
                balance = PrepaidUtil.available_balance(
                    self.instance.msisdn, self.instance.is_not_mobile_subscription
                )

            pkg = subscription_util_obj.package
            if pkg.recurrence == Package.RECURRENT:
                fee_item = pkg.activation_fee
            else:
                fee_item = pkg.fee_item
            if fee_item and fee_item.price > balance:
                return True

        except HttpErrorResponse as error:
            raise ValidationError(error)
        return False

    @cached_property
    def _is_package_reactivation(self) -> bool:
        """Check for package reactivation. """
        return (
            self.instance.xref_packages.active(package=self.package).exists()
            and self.package.re_activation
        )

    @classmethod
    def get_scheduling_field(
        cls, instance: Optional[Type[Model]] = None
    ) -> Dict[str, Any]:
        """
        :param instance: a Subscription
        :returns: Returns dictionary containing field name as key
            and forms.ChoiceField containing time choices
            and forms.DateField with the time.
        """
        if (
            cls.package
            and cls.package.has_tag(PACKAGE_TAG_ACTIVATE_NEXT_MIDNIGHT)
            and not cls.base_fields['is_enabled'].initial  # type: ignore
        ):
            schedule_time_field = forms.ChoiceField(
                widget=forms.RadioSelect,
                choices=SCHEDULE_TIME_NEXT_MIDNIGHT,
                required=False,
                initial=SCHEDULE_TIME_NEXT_MIDNIGHT[0][0],
                label=ugettext_lazy('Schedule time'),
            )
        elif (
            cls.package
            and cls.package.product_family
            and cls.package.has_variant(ProductFamily.PRODUCT_VARIANT_MONTHLY_SCHEDULED)
        ) and (
            cls.base_fields['is_enabled'].initial  # type: ignore
            or instance.xref_packages.active(  # type: ignore
                package__product_family=cls.package.product_family
            ).exists()
        ):
            schedule_time_field = forms.ChoiceField(
                widget=forms.RadioSelect,
                choices=SCHEDULE_TIME_FIRST_OF_MONTH_AND_IMMEDIATE,
                required=False,
                initial='first_of_month',
                label=ugettext_lazy('Schedule time'),
            )
        else:
            schedule_time_field = forms.ChoiceField(
                widget=forms.RadioSelect,
                choices=SCHEDULE_TIME_CHOICES,
                required=False,
                initial='immediately',
                label=ugettext_lazy('Schedule time'),
            )

        return {
            'is_scheduled': schedule_time_field,
            'scheduled_date': forms.DateField(
                widget=forms.DateInput(
                    format='%d.%m.%Y',
                    attrs={
                        'class': 'datepicker',
                        "min": add_to_date(days=1).date(),
                        "max": add_to_date(
                            years=PACKAGE_MAX_CHANGE_SCHEDULABLE_YEARS
                        ).date(),
                        "value": add_to_date(days=1).date(),
                        'onkeypress': 'return false',
                    },
                ),
                input_formats=['%d.%m.%Y', '%Y.%m.%d', '%Y-%m-%d', '%d-%m-%Y'],
                required=False,
                label=l_('Schedule date'),
            ),
        }

    def schedule_update_required(self):
        if (
            self.package
            and self.package.product_family
            and self.package.has_variant(
                ProductFamily.PRODUCT_VARIANT_MONTHLY_SCHEDULED
            )
        ) and (
            self.base_fields['is_enabled'].initial  # type: ignore
        ):
            return True
        return False

    def get_schedule(self, data: Dict[str, Any]) -> datetime:
        scheduled_at = data.pop('scheduled_date', None)
        is_scheduled = data.pop("is_scheduled", None)
        if (
            self.package.recurrence == Package.RECURRENT
            and not data.get("is_enabled", True)
            and self.instance.is_non_rtcc
            and not is_scheduled
        ):
            return end_of_day()
        if is_scheduled != 'immediately' and scheduled_at:
            if (
                not data.get("is_enabled", True)
                and self.package
                and self.package.product_family
                and self.package.has_variant(
                    ProductFamily.PRODUCT_VARIANT_MONTHLY_SCHEDULED
                )
            ):
                # Scheduling memo at random time between 21:00hrs to 23:30hrs.
                return first_of_next_month_local_with_offset(
                    minutes=MONTHLY_SCHEDULED_OFFSET_IN_MINUTES,
                    seconds=randrange(-9000, 0, 1),
                )
            else:
                if data.get("is_enabled", True):
                    time_for_execution = datetime.min.time()
                else:
                    time_for_execution = datetime.max.time()
                if self.instance.is_non_rtcc:
                    return add_to_date(
                        base=make_aware(
                            datetime.combine(scheduled_at, time_for_execution),
                            get_default_timezone(),
                        )
                    )
                if self.instance.is_postpaid and self.instance.is_mobile_gsm:
                    return add_to_date(
                        base=make_aware(
                            datetime.combine(scheduled_at, time_for_execution),
                            get_default_timezone(),
                        ),
                        seconds=randrange(-9000, 0, 1),
                    )
                return make_aware(
                    datetime.combine(scheduled_at, datetime.min.time()),
                    get_default_timezone(),
                )
        return datetime_now()

    def get_changes(self, data):
        """
        Convert form data to structured dictionary with
        package and service codes and service attribute values

        :param dict data: Change data.
        :return: Package code (key) with bool|dict value.
        :rtype: dict
        """

        if (
            not self.package.service_channels.filter(
                code=settings.DEFAULT_CHANNEL
            ).exists()
            or not data
        ):
            return {}

        is_enabled = data.pop('is_enabled', None)

        if is_enabled is None:
            is_enabled = (
                self.instance.xref_packages.active()
                .select_related('package')
                .filter(package__code=self.package.code)
                .exists()
            )
        attributes = {}

        if self.is_included:
            is_enabled = True

        if is_enabled:
            packages = {self.package.code: {}}
            if self.package.services.count() > 0:
                for pkg_srv in self.package.services.all():
                    service_code = pkg_srv.service.code
                    for key, value in list(data.items()):
                        if key.startswith(service_code):
                            data.pop(key)
                            attr = key.replace('{}_'.format(service_code), '', 1)
                            attributes[attr] = value

                    if attributes:
                        packages[self.package.code][service_code] = attributes
                if not packages[self.package.code]:
                    packages[self.package.code] = True
            else:
                packages = {self.package.code: True}
        else:
            packages = {self.package.code: False}
        # Included package with attributes, changes only to attrs
        if self.is_included and attributes:
            return packages
        # Included package without attributes, no changes made
        elif self.is_included and not attributes:
            return
        else:
            return packages

    def should_skip_balance(self, changes, extra_changes):
        if self.package.has_tag_in(
            [
                PACKAGE_TAG_FTTB_CMSANDCS,
                PACKAGE_TAG_FTTB_CMSANDCS_ACTIVATION,
                PACKAGE_TAG_SKIPBALANCE_TRIAL_DURATION,
            ]
        ):
            package_id, service_values = changes.popitem()
            pkg_change = self.instance.normalize_package_change(
                package_id,
                service_values,
                self.package,
                re_activated=extra_changes.get('re_activated', False),
            )
            if pkg_change:
                pkg_code, (old_vals, new_vals) = pkg_change.popitem()
                if self.package.has_tag(PACKAGE_TAG_SKIPBALANCE_TRIAL_DURATION):
                    for service_value in new_vals.values():
                        for (
                            service_attr_key,
                            service_attr_value,
                        ) in service_value.items():
                            if (
                                service_attr_key == TRIAL_DURATION
                                and service_attr_value
                                and int(service_attr_value) > 0
                            ):
                                return True
                return (
                    old_vals is None
                    or (
                        self.package.has_tag(PACKAGE_TAG_CMS_SERVICES)
                        and new_vals[VISITSERVICE].get(VISITCONFIRMED)
                        == SERVICE_ATTRIBUTE_VALUE_CANCEL
                    )
                    or new_vals.get(DEVICE_SERVICE, {}).get(
                        SERVICE_ATTRIBUTE_PRODUCT_STATUS
                    )
                    == STATE_WAITING_FOR_ACTIVATE
                )
        return False

    def send_sms_for_ongoing_deactivation(
        self, package: Package, subscription: Subscription
    ):
        """
        Method to send sms for ongoing deactivation requests

        :param package: package
        :param subscription: subscription
        """
        package_code = package.code
        template_id = ''
        if package_code in [FMCKYIVTVPACKIVI, FTTBKYIVTVPACKIVI]:
            template_id = POP_FTTB_TVPACK_IVI_DEACTIVATION
        elif (
            subscription.type.line_type == LINE_TYPE_FTTB
            and package.product_family.name == PRODUCT_FAMILY_HOMETV
        ):
            if package_code == FTTBHMTVKINOHIT:
                template_id = HOMETV_DEACTIVATION_WITH_KINOHIT
            else:
                template_id = HOMETV_DEACTIVATION_WITHOUT_KINOHIT
        if template_id:
            ErrandsClient.send_sms(
                msisdn=NotificationUtil.get_notification_msisdn(subscription),
                template_id=template_id,
                language=subscription.language.lower(),
                context={},
                task_id=f"send_sms_{uuid4().hex}",
            )

    def save(self, request, **kwargs):
        self.cleaned_data.pop('message', '')
        scheduled_at = self.get_schedule(self.cleaned_data)
        logger.error(
            f'cleaned data for msisdn:: {self.instance.msisdn} : {self.cleaned_data}'
        )
        logger.error(
            f'changed data for msisdn:: {self.instance.msisdn} : {self.changed_data}'
        )
        if not self.changed_data:
            return
        changes = self.get_changes(self.cleaned_data)
        logger.error(
            f'changes need to done for msisdn:: {self.instance.msisdn} : {changes}'
        )
        if not changes:
            return
        multiplier_value = self.instance.get_multiplier_value(self.package)
        multiplier_packge = get_multiplier(self.package)
        extra_changes = {
            "reseller": self.fp_action.user.salesman.resellers.first().code
        }
        if multiplier_value:
            changes.pop("multiplier", None)
        elif (
            not multiplier_value
            and self.cleaned_data.get("multiplier")
            and multiplier_packge
        ):
            extra_changes["multiplier_value"] = int(
                float(self.cleaned_data["multiplier"])
            )
            extra_changes["multiplier_code"] = multiplier_packge.code
            extra_changes["valid_from"] = datetime_now()
            extra_changes["valid_to"] = parse_datetime(settings.INFINITE_DATETIME)
        extra_changes.update({"reason": self._get_reason()})
        extra_changes.update(self._change_data(self.cleaned_data))
        if self.cleaned_data.get("is_slave_sim") is False:
            extra_changes["is_slave_sim"] = False
        if self.cleaned_data.get(IS_REMOTE_INTERNET_SIM_PAIR_ACTIVATION):
            extra_changes[IS_REMOTE_INTERNET_SIM_PAIR_ACTIVATION] = True
        # set re_activated if package already active and package has
        # re_activation allowed. (In the later case it might just be an update
        # of service attributes)
        if self._is_package_reactivation:
            extra_changes['re_activated'] = True
        if self.cleaned_data.get('pending_order', False):
            extra_changes['is_pending_order'] = True

        package_eligibility_util = SubscriptionPackageEligibilityUtil(
            subscription=self.instance, package=self.package, actor=request.user
        )
        # If "replace" conflict type is applied, then need to deactivate conflicted packages first
        active_conflict_packages = package_eligibility_util.get_active_conflicted_packages(
            CONFLICT_TYPE_REPLACE
        )
        for acp in active_conflict_packages:
            deactivation_changes = {acp.package.code: False}
            self.instance.request_package_update(
                actor=request.user,
                changes=deactivation_changes,
                scheduled_at=scheduled_at,
                extra_changes={
                    "reseller": self.fp_action.user.salesman.resellers.first().code
                },
                channel=settings.DEFAULT_CHANNEL,
                **kwargs,
            )

        memo = self.instance.request_package_update(
            actor=request.user,
            changes=changes,
            scheduled_at=scheduled_at,
            extra_changes=extra_changes,
            channel=settings.DEFAULT_CHANNEL,
            **kwargs,
        )
        logger.error(f'memo to execute for msisdn:: {self.instance.msisdn} : {memo}')
        if memo:
            is_package_deactivation = memo.change.get("is_deactivated", False)
            if memo.is_ongoing and is_package_deactivation:
                self.send_sms_for_ongoing_deactivation(
                    package=self.package, subscription=self.instance
                )

    class Meta:
        title = 'This is title'
        handler = PackageHandler
        allow_scheduling = True
        onscheduled_form = OnchangePackageScheduledForm


class BasePackageAction(TemplateAction):
    package = None

    change_scheduled_button = l_('Change scheduled')

    def __init__(self, instance, tab):
        self.tab = tab
        self.instance = instance
        assert self.form is not None
        self.key = self.form.meta.key
        if self.target_instance:
            self.key += '_%s_%s' % (
                self.target_instance._meta.model_name,
                self.target_instance.pk,
            )
        self.form_class = self.get_form_class(tab.ongoing_changes)
        if (
            self.form_class
            and self.form_class.is_ongoing
            and not self.form_class.memo.is_cancel_failed()
        ):
            if self.form_class.memo.is_cancel_requested():
                self.edit_button = self.canceled_button
            elif self.form_class.memo.status == 'scheduled':
                self.edit_button = self.change_scheduled_button
            else:
                self.edit_button = self.ongoing_button

    @staticmethod
    def format_ongoing_changes(all_changes: Dict[str, List[int]]) -> Dict[str, Memo]:
        """
        Changes 'services_<handler_name> to services_<package_name>,
        so that, get_is_ongoing can confirm whether this service
        is an ongoing change or not.

        :return: Dict object containing ongoing service changes
        :rtype: Dict
        """
        result = {}
        memo_ids = [
            value
            for key, value in all_changes.items()
            if key.startswith("service")
            or key.startswith("product_family_package_change")
        ]
        if memo_ids:
            flat_memo_ids = [memo_id for sublist in memo_ids for memo_id in sublist]
            for memo in Memo.objects.filter(id__in=flat_memo_ids).exclude(
                status__in=[Memo.STATUS_ERROR, Memo.STATUS_FAILED]
            ):
                packages = memo.change.get('packages', [])
                for package_key in packages:
                    if isinstance(package_key, dict):
                        for key in package_key.keys():
                            package_key = key
                    key = 'package_{}'.format(package_key)
                    result[key] = memo
        return result

    def get_is_ongoing(self, all_changes):
        changes = self.format_ongoing_changes(all_changes)
        key = 'package_{}'.format(self.package.code)
        return changes.get(key, None)

    def get_form_class(self, all_changes):
        ongoing_memo = self.get_is_ongoing(all_changes)
        if ongoing_memo is not None:
            ongoing_form = self.get_ongoing_form(
                memo=ongoing_memo, _form_target=self.target_instance
            )
            return ongoing_form
        return self.get_change_form(_form_target=self.target_instance)

    def get_ongoing_form(self, **extra):
        extra['fp_action'] = self
        if extra['memo'].status == 'scheduled':
            form = extra.get('onscheduled_form', BasePackageForm.Meta.onscheduled_form)
        else:
            form = extra.get('ongoing_form', self.form.meta.ongoing_form)
            choices = form.action_choice(**extra)
            extra['ongoing_action'] = forms.ChoiceField(choices=choices)
        return type('Patched' + form.__name__, (form,), extra)

    def has_permission(self, user):
        return user.has_perm('contracts.can_add_package')

    def has_barring_restrictions(self):
        if (
            self.instance.contract.is_suspended
            and self.instance.validate_package_allowed_for_suspended(
                package=self.package
            )
        ):
            return False
        if self.package.has_tag(PACKAGE_TAG_SKIP_BARRING_CHECK):
            return False
        return super().has_barring_restrictions()


def get_package_action(sub, package, is_enabled, is_included, is_crm_enabled=True):
    """
    Return dynamically created Action and Form connected to it
    """

    language = translation.get_language()
    services = customers.contracts.tabs.contract.services
    if (
        sub.contract.status in [Contract.ACTIVE, Contract.SUSPENDED]
        and package.code == FTTB_OPTION82_PACKAGE
        and sub.xref_packages.active(package=package).exists()
    ):
        is_crm_enabled = False

    if not sub or not package:
        return None
    if not sub.validate_package_allowed_for_suspended(package=package):
        return None
    package_name = package.get_commercial_name_display()
    # Disable included packages on/off switching
    # Disable the package on/off switching,
    # because CRM is not enabled in PC
    if is_included or not is_crm_enabled:
        read_only_bool_field = forms.BooleanField(
            label=ugettext_lazy(
                package_name + f"({get_package_price(package=package)})"
            ),
            required=False,
            initial=is_enabled,
        )
        read_only_bool_field.widget.attrs['disabled'] = True
        title = _('Included packages cannot be switched off')
        if not is_crm_enabled:
            title = _("Package can't be switched off, If CRM isn't enabled in PC")
        read_only_bool_field.widget.attrs['title'] = title
        form_fields = {'is_enabled': read_only_bool_field, 'package': package}
        if (
            package
            and not is_enabled
            and package.has_tag(PACKAGE_TAG_ACTIVATE_NEXT_MIDNIGHT)
        ):
            schedule_time_field = forms.ChoiceField(
                widget=forms.RadioSelect,
                choices=SCHEDULE_TIME_NEXT_MIDNIGHT,
                required=False,
                initial=SCHEDULE_TIME_NEXT_MIDNIGHT[0][0],
                label=ugettext_lazy('Schedule time'),
            )
        elif (
            package
            and package.product_family
            and package.has_variant(ProductFamily.PRODUCT_VARIANT_MONTHLY_SCHEDULED)
        ):
            schedule_time_field = forms.ChoiceField(
                widget=forms.RadioSelect,
                choices=SCHEDULE_TIME_FIRST_OF_MONTH_AND_IMMEDIATE,
                required=False,
                initial='immediately',
                label=ugettext_lazy('Schedule time'),
            )
        else:
            schedule_time_field = forms.ChoiceField(
                widget=forms.RadioSelect,
                choices=SCHEDULE_TIME_CHOICES,
                required=False,
                initial='immediately',
                label=ugettext_lazy('Schedule time'),
            )
        form_fields.update({'is_scheduled': schedule_time_field, 'package': package})
        schedule_date_field = forms.DateField(
            widget=forms.DateInput(
                format='%d.%m.%Y',
                attrs={'readonly': 'readonly', 'onkeypress': "return false"},
            ),
            input_formats=['%d.%m.%Y'],
            required=False,
            label=ugettext_lazy('Schedule date'),
        )
        schedule_date_field.widget.attrs['readonly'] = True
        form_fields.update({'scheduled_date': schedule_date_field, 'package': package})
    else:
        # To display list of active child packages which will be
        # deactivated if parent package is deactivated
        active_package_codes = sub.enabled_package_codes
        child_packages = ChildPackageUtil.child_packages(
            package_codes=[package.code],
            sub_type=sub.type,
            checked_package_codes=None,
            parent_deactivation=True,
        )
        active_child_packages = [
            pkg_code for pkg_code in child_packages if pkg_code in active_package_codes
        ]
        has_child_packages = len(active_child_packages) > 0
        active_child_packages_names = []
        if has_child_packages:
            active_child_packages_names = [
                pkg.get_commercial_name_display()
                for pkg in Package.objects.filter(code__in=active_child_packages)
            ]
        form_fields = {
            'is_enabled': forms.BooleanField(
                label=ugettext_lazy(
                    package_name + f"({get_package_price(package=package)})"
                ),
                required=False,
                initial=is_enabled,
                widget=forms.CheckboxInput(
                    attrs={
                        'has_child_packages': str(has_child_packages),
                        'code': package.code,
                    }
                ),
                help_text=ugettext_lazy(
                    'If'
                    + package_name
                    + ' is deactivated then associated Child packages ('
                    + (', '.join(active_child_packages_names))
                    + ') will also be deactivated.'
                ),
            ),
            'package': package,
        }
        form_fields.update(
            {
                'scheduled_date': forms.DateField(
                    widget=forms.DateInput(
                        format='%d.%m.%Y',
                        attrs={'readonly': 'readonly', 'onkeypress': "return false"},
                    ),
                    input_formats=['%d.%m.%Y'],
                    required=False,
                    label=ugettext_lazy('Schedule date'),
                ),
                'package': package,
            }
        )
    attr_fields = sub.get_attribute_fields(package)
    form_fields.update(attr_fields)
    bases = (BasePackageForm,)
    form = type(
        '{}ActivePackageForm'.format(str(package.id))
        if is_enabled
        else '{}InactivePackageForm'.format(str(package.id)),
        bases,
        form_fields,
    )

    if is_enabled and package.warning_deactivation_message.get(language):
        form.meta.help_text = mark_safe(
            package.warning_deactivation_message.get(language)
        )
    elif not is_enabled and package.warning_activation_message.get(language):
        form.meta.help_text = mark_safe(
            package.warning_activation_message.get(language)
        )

    form = PackageUtil.get_extra_info(package, form, is_enabled)
    return type(
        '{}ActivePackageAction'.format(str(package.id))
        if is_enabled
        else '{}InactivePackageAction'.format(str(package.id)),
        (BasePackageAction,),
        {'form': form, 'allow_message': False, 'package': package},
    )


class BasePackageChangeForm(
    ReasonSelectFormMixin, MultiplierPackageFormMixin, FourpaneForm
):
    """
    Base form to change packages inside a product family where
    only one package can be active. The change (disable the active one
    and activate the other) is scheduled immediately.

    To use this outside CM, include following:
        self.changes = {'packages': [{old_pkg.code: {}}, {new_pkg.code: {}}]}

    Note that the requirement for the same product family is not
    actually checked anywhere because the change is made possible
    within a group where packages belong to the same family.
    If this should change, please include the relevant test.
    """

    package = None
    validators = None
    changes = None

    class Meta:
        title = 'This is the title'
        handler = ProductFamilyPackageChangeHandler
        allow_scheduling = True
        description = "Change packages"
        onscheduled_form = OnchangePackageScheduledForm

    def __init__(self, *args, **kwargs):
        super(BasePackageChangeForm, self).__init__(*args, **kwargs)
        package = self.fp_action.package
        multiplier = get_multiplier(package)
        is_package_activated = self.instance.xref_packages.active(
            package__code=package.code
        )
        self.initial.update(
            PackageUtil.get_initial_info(package, self, is_package_activated)
        )
        if self.instance.is_fixed_line_subscription and multiplier:
            multiplier_value = self.instance.get_multiplier_value(package)
            self.fields["multiplier"] = forms.CharField(
                widget=forms.TextInput(attrs={'type': 'number'})
            )
            self.fields["multiplier"].initial = int(multiplier.multiplier_value)
            if multiplier_value:
                self.fields["multiplier"].widget.attrs['readonly'] = True
                self.fields["multiplier"].initial = multiplier_value
        if self.fp_action.tab.allow_additional_params(package):
            self.fp_action.tab.alter_additional_params(package, self.fields)
            for package_service in package.services.all():
                service = package_service.service
                active_services = SubscriptionUtil.get_active_services(self.instance)
                for active_service in active_services:
                    if active_service.code == service.code:
                        attributes = SubscriptionUtil.get_active_service_attributes_of_service(
                            subscription=self.instance, service=active_service
                        )
                        for key, value in attributes.items():
                            self.fields.get(
                                '{}_{}'.format(service.code, key)
                            ).initial = value
        for field in self.fields:
            if (
                STATIC_IP_FIELD.lower() in field.lower()
                or STATIC_IP_SESSION_TO_FIELD.lower() in field.lower()
            ):
                self.fields[field].widget = forms.HiddenInput()
                self.fields[field].widget.required = False
                self.fields[field].required = False
                self.fields[field].widget.readonly = 'readonly'

    def clean(self):
        package = self.fp_action.package
        if not self.data.get(STATIC_IP_FIELD):
            static_ip_data = PackageUtil.get_static_ip_address(self, package)
            self.cleaned_data.update(static_ip_data)
            self.initial.update(static_ip_data)
        if self.fp_action.tab.allow_additional_params(self.fp_action.package):
            # if form has other allowed parameters, validate additional
            # parameters
            try:
                self.fp_action.tab.validate_additional_params(
                    self.fp_action.package, self.cleaned_data
                )
            except ValidationError as err:
                raise ValidationError(err)

        package_eligibility_util = SubscriptionPackageEligibilityUtil(
            subscription=self.instance,
            package=self.fp_action.package,
            actor=self.fp_action.tab.request.user.username
            if self.fp_action.tab.request
            else '',
            channel=settings.DEFAULT_CHANNEL,
        )
        package_eligibility_util._validate_package_and_barring_conflicts()

        return super(BasePackageChangeForm, self).clean()

    def get_changes(self, data):
        if self.fp_action.tab.allow_additional_params(self.fp_action.package):
            attributes = {}
            old_packages, new_packages = self.changes.get('packages')
            package = self.fp_action.package
            if package.services.count() > 0:
                for pkg_srv in package.services.all():
                    service_code = pkg_srv.service.code
                    for key, value in list(data.items()):
                        if key.startswith(service_code):
                            data.pop(key)
                            attr = key.replace('{}_'.format(service_code), '', 1)
                            attributes[attr] = value
                    if attributes:
                        new_packages[package.code][service_code] = attributes
                        if package.code in old_packages:
                            old_packages[package.code][
                                service_code
                            ] = SubscriptionUtil.get_active_service_attributes_of_service(
                                subscription=self.instance, service=pkg_srv.service
                            )
        return self.changes

    @classmethod
    def get_scheduling_field(
        cls, instance: Optional[Type[Model]] = None
    ) -> Dict[str, Any]:
        """
        :param instance: a Subscription
        :returns: Returns dictionary containing field name as key
            and forms.ChoiceField containing time choices
            and forms.DateField with the time.
        """
        if (
            cls.package
            and cls.package.product_family
            and cls.package.has_variant(ProductFamily.PRODUCT_VARIANT_MONTHLY_SCHEDULED)
        ) and (
            instance.xref_packages.active(  # type: ignore
                package__product_family=cls.package.product_family
            ).exists()
        ):
            schedule_time_field = forms.ChoiceField(
                widget=forms.RadioSelect,
                choices=SCHEDULE_TIME_FIRST_OF_MONTH_ONLY,
                required=False,
                initial='first_of_month',
                label=ugettext_lazy('Schedule time'),
            )
        else:
            schedule_time_field = forms.ChoiceField(
                widget=forms.RadioSelect,
                choices=SCHEDULE_TIME_CHOICES,
                required=False,
                initial='immediately',
                label=ugettext_lazy('Schedule time'),
            )

        return {
            'is_scheduled': schedule_time_field,
            'scheduled_date': forms.DateField(
                widget=forms.DateInput(
                    format='%d.%m.%Y',
                    attrs={
                        'type': 'date',
                        "min": add_to_date(days=1).date(),
                        "max": add_to_date(
                            years=PACKAGE_MAX_CHANGE_SCHEDULABLE_YEARS
                        ).date(),
                        "value": add_to_date(days=1).date(),
                    },
                ),
                input_formats=['%d.%m.%Y', '%Y.%m.%d', '%Y-%m-%d'],
                required=False,
                label=l_('Schedule date'),
            ),
        }

    def get_schedule(self, data):
        scheduled_at = data.pop('scheduled_date', None)
        is_scheduled = data.pop("is_scheduled", None)
        package = self.fp_action.package
        subscription = self.instance

        if is_scheduled != 'immediately' and scheduled_at:
            if (
                package
                and package.product_family
                and package.has_variant(ProductFamily.PRODUCT_VARIANT_MONTHLY_SCHEDULED)
            ):
                return (
                    first_of_next_month_local_with_offset(
                        minutes=MONTHLY_SCHEDULED_OFFSET_IN_MINUTES
                    ),
                    True,
                )
            else:
                return (
                    make_aware(
                        datetime.combine(scheduled_at, datetime.min.time()),
                        get_default_timezone(),
                    ),
                    False,
                )
        elif (
            package.product_family
            and package.has_variant(ProductFamily.PRODUCT_VARIANT_MONTHLY_SCHEDULED)
            and package.product_family.max_choices == 1
        ):
            active_sub_pkg = subscription.get_current_product_family_package(
                package.product_family
            )
            if active_sub_pkg and active_sub_pkg.package.code != package.code:
                # Scheduling memo at random time between 21:00hrs to 23:30hrs.
                return (
                    first_of_next_month_local_with_offset(
                        minutes=MONTHLY_SCHEDULED_OFFSET_IN_MINUTES,
                        seconds=randrange(-9000, 0, 1),
                    ),
                    True,
                )
            else:
                return datetime_now(), False
        return datetime_now(), False

    def has_changed(self):
        return True

    def save(self, request, **kwargs):
        changes = self.get_changes(self.cleaned_data)
        package = self.fp_action.package
        if not changes:
            return
        multiplier_value = self.instance.get_multiplier_value(package)
        multiplier_packge = get_multiplier(package)
        if multiplier_value:
            changes.pop("multiplier", None)
        elif (
            not multiplier_value
            and self.cleaned_data.get("multiplier")
            and multiplier_packge
        ):
            changes["multiplier_value"] = int(float(self.cleaned_data["multiplier"]))
            changes["multiplier_code"] = multiplier_packge.code
            changes["valid_from"] = datetime_now()
            changes["valid_to"] = parse_datetime(settings.INFINITE_DATETIME)
        message = self.cleaned_data.pop('message', '')
        changes.update({"reason": self._get_reason()})
        scheduled_at, scheduled_eom = self.get_schedule(self.cleaned_data)
        if scheduled_eom:
            changes.update(
                {"schedule_deactivation_for": end_of_day(end_of_this_month())}
            )
            changes.update({"schedule_for": first_of_next_month_local()})
        charge = self.get_charge(self.cleaned_data)
        handler = self.meta.handler.name
        newtlet = self.fp_action.tab.newtlet.name
        memo = self.instance.request_update(
            request.user,
            changes,
            charge,
            handler,
            scheduled_at,
            newtlet,
            message,
            **kwargs,
        )
        if not memo._is_scheduled_now() and package.code in POP_FTTB_TVPACKS_CHOICES:
            notification_sms_config(
                TEMPLATE_POP_FTTB_TVPACK_ORDER_TO_CHANGE,
                request.user,
                self.instance,
                create_memo=False,
            )
        return memo


class BasePackageChangeAction(TemplateAction):
    package = None

    change_scheduled_button = l_('Change scheduled')

    def get_is_ongoing(self, all_changes):
        changes = self.format_ongoing_changes(all_changes)
        key = 'package_{}'.format(self.package.code)
        return changes.get(key, None)

    def has_permission(self, user):
        return user.has_perm('contracts.can_edit_package')

    def __init__(self, instance, tab):
        self.tab = tab
        self.instance = instance
        assert self.form is not None
        self.key = self.form.meta.key
        if self.target_instance:
            self.key += '_%s_%s' % (
                self.target_instance._meta.model_name,
                self.target_instance.pk,
            )
        self.form_class = self.get_form_class(tab.ongoing_changes)
        if (
            self.form_class
            and self.form_class.is_ongoing
            and not self.form_class.memo.is_cancel_failed()
        ):
            if self.form_class.memo.is_cancel_requested():
                self.edit_button = self.canceled_button
            elif self.form_class.memo.status == 'scheduled':
                self.edit_button = self.change_scheduled_button
            else:
                self.edit_button = self.ongoing_button

    @staticmethod
    def format_ongoing_changes(all_changes: Dict[str, List[int]]) -> Dict[str, Memo]:
        """
        Changes 'services_<handler_name> to services_<package_name>,
        so that, get_is_ongoing can confirm whether this service
        is an ongoing change or not.

        :return: Dict object containing ongoing service changes
        :rtype: Dict
        """
        result = {}
        memo_ids = [
            value
            for key, value in all_changes.items()
            if key.startswith("service")
            or key.startswith("product_family_package_change")
        ]
        if memo_ids:
            flat_memo_ids = [memo_id for sublist in memo_ids for memo_id in sublist]
            for memo in Memo.objects.filter(id__in=flat_memo_ids).exclude(
                status__in=[Memo.STATUS_ERROR, Memo.STATUS_FAILED]
            ):
                packages = memo.change.get('packages', [])
                for package_key in packages:
                    if isinstance(package_key, dict):
                        for key in package_key.keys():
                            package_key = key
                    key = 'package_{}'.format(package_key)
                    result[key] = memo
        return result

    def get_form_class(self, all_changes):
        ongoing_memo = self.get_is_ongoing(all_changes)
        if ongoing_memo is not None:
            ongoing_form = self.get_ongoing_form(
                memo=ongoing_memo, _form_target=self.target_instance
            )
            return ongoing_form
        return self.get_change_form(_form_target=self.target_instance)

    def get_ongoing_form(self, **extra):
        extra['fp_action'] = self
        if extra['memo'].status == 'scheduled':
            form = extra.get(
                'onscheduled_form', BasePackageChangeForm.Meta.onscheduled_form
            )
        else:
            form = extra.get('ongoing_form', self.form.meta.ongoing_form)
            choices = form.action_choice(**extra)
            extra['ongoing_action'] = forms.ChoiceField(choices=choices)
        return type('Patched' + form.__name__, (form,), extra)

    def has_barring_restrictions(self):
        if (
            self.instance.contract.is_suspended
            and self.instance.validate_package_allowed_for_suspended(
                package=self.package
            )
        ):
            return False
        if self.package.has_tag(PACKAGE_TAG_SKIP_BARRING_CHECK):
            return False
        return super().has_barring_restrictions()


# TODO: why we have two of these functions to dynamically generate action & form
# see get_package_action from above ... get rid of this?
def get_package_change_action(sub, new_pkg, old_pkg, is_active=False):
    # TODO: This is almost exact copy of get_package_price
    # originally had same docstring so it's copied ...
    if not sub or not new_pkg or not old_pkg:
        return None

    cur_pkg = new_pkg
    if is_active:
        # This form is generated for package that is
        # active and is in status to be replaced.
        cur_pkg = old_pkg
    # TODO clean_pna_form, currently not supported (data and travel
    # don't need this)
    form_fields = {"package": cur_pkg}
    attr_fields = sub.get_attribute_fields(cur_pkg)
    # TODO add support for forms that contains additional parameters
    form_fields.update(attr_fields)
    form = type(
        '{}ActivePackageForm'.format(str(cur_pkg.id))
        if is_active
        else '{}InactivePackageForm'.format(str(cur_pkg.id)),
        (BasePackageChangeForm,),
        form_fields,
    )
    form.changes = {
        'packages': [{old_pkg.code: {}}, {new_pkg.code: {}}],
        'channel': settings.DEFAULT_CHANNEL,
    }
    form = PackageUtil.get_extra_info(new_pkg, form, False)
    action = type(
        '{}ActivePackageAction'.format(str(cur_pkg.id))
        if is_active
        else '{}InactivePackageAction'.format(str(cur_pkg.id)),
        (BasePackageChangeAction,),
        {
            'form': form,
            'allow_message': False,
            'package': cur_pkg,
            'edit_button': _('Activate...'),
        },
    )

    form.meta.description = _('Activate package')
    if old_pkg == new_pkg:
        form.meta.help_text = mark_safe(
            '{}: <i class="u">"{}"{}</i>.'.format(
                _('Re-activate currently activated package'),
                old_pkg.get_commercial_name_display(),
                get_package_price(package=old_pkg),
            )
        )
        form.changes['re_activated'] = True
    else:
        form.meta.help_text = mark_safe(
            '{}: <i class="u">"{}"{}</i> {} <i class="u">"{}"{}</i>?'
            '.'.format(
                _('Replace currently activated package'),
                old_pkg.get_commercial_name_display(),
                get_package_price(package=old_pkg),
                _('with'),
                new_pkg.get_commercial_name_display(),
                get_package_price(package=new_pkg),
            )
        )
    return action


class PreActivatedPackageForm(FourpaneForm):
    template = 'fourpane/remove_pre_activated_package_edit_form.html'

    class Meta:
        handler = PackageHandler
        allow_scheduling = False

    def __init__(self, *args, **kwargs):
        super(PreActivatedPackageForm, self).__init__(*args, **kwargs)

    def information_for_deletion(self):
        """
        Method to return the package information for deletion

        :return: information regarding deletion
        """
        return l_(
            f"The package {self.package.package.get_commercial_name_display()} "
            f"will be deleted and will not be activated post subscription "
            f"activation"
        )

    def save(self, request, **kwargs):
        """
        Method to save the form
        """
        changes = {self.package.package.code: False}
        extra_changes = {"provisioning": False, "delete_only": True}
        scheduled_at = datetime_now()
        self.instance.request_package_update(
            actor=request.user,
            changes=changes,
            scheduled_at=scheduled_at,
            extra_changes=extra_changes,
            channel=settings.DEFAULT_CHANNEL,
        )


class PreActivatedPackageAction(TemplateAction):
    form = PreActivatedPackageForm
    allow_message = False
    allow_scheduling = False
    save_button: Optional[str] = l_('Confirm')

    def get_form_class(self, all_changes):
        ongoing_memo = self.get_is_ongoing(all_changes)
        if ongoing_memo is not None:
            return self.get_ongoing_form(
                memo=ongoing_memo, package=self.target_instance
            )
        return self.get_change_form(package=self.target_instance)
