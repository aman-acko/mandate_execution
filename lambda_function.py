import json
from datetime import datetime, timedelta

import requests as requests
import structlog as structlog

logger = structlog.get_logger()
MOTOR_ORCHESTRATOR_URL = "http://motor-orchestrator-uat.internal.ackodev.com"
PAYMENT_URL = "https://payment-service-uat.ackodev.com"
R2D2_SERVICE_URL = "http://r2d2.internal.ackodev.com"
COUPON_SERVICE = "http://consumer-service-uat.internal.ackodev.com"


def opt_out_mandate(proposal_ekey):
    url = MOTOR_ORCHESTRATOR_URL + "/motororchestrator/api/v1/renewals/mandate/opt-out"
    payload = {
        "proposal_id": proposal_ekey,
        "consent": True
    }
    header = {
        'Cookie': 'custom'
    }
    response = requests.post(url, headers=header, json=payload)
    return response


def get_latest_ekey(schedule):
    schedules = schedule.get('schedules')
    latest_schedule_ref_id = None
    try:
        latest_schedule_id = -1
        for i in range(len(schedules)):
            current_schedule = schedules[i]
            current_schedule_id = current_schedule.get('schedule_id')
            if latest_schedule_id < current_schedule_id:
                latest_schedule_id = current_schedule_id
                latest_schedule_ref_id = current_schedule.get('schedule_reference_id', {})
    except Exception as e:
        print(e)
    return latest_schedule_ref_id


def decrypt_id(id, table_name):
    logger.info(f"Get ekey for id: {id} and table name: {table_name}")
    params = {"ekey": id, "tn": table_name}
    response = requests.get(
        f"{COUPON_SERVICE}/admin/api/v1/ekey/dec", params=params
    )

    if response.status_code == 200:
        logger.info("Encrypt API succeeded")
        return response.text
    else:
        try:
            response_data = response.json()
        except ValueError:
            response_data = response.text
        logger.info(f"Encrypt API failed with response: {response_data}")


def cancelled_mandate(data):
    try:
        payment_plan_id = data.get('payments_plan_id')
        schedule = get_schedule(payment_plan_id)
        proposal_ekey = get_latest_ekey(schedule)
        proposal_id = decrypt_id(proposal_ekey, 'auto_proposal')
        url = MOTOR_ORCHESTRATOR_URL + "/motororchestrator/api/v1/renewals/mandate/cancel"
        payload = {
            "proposal_id": proposal_id,
            "reason": "Mandate Failed by User"
        }
        response = requests.post(url, json=payload)
        return response
    except Exception as e:
        print(e)


def validate_mandate_proposal(proposal_id):
    logger.info(
        f"validate_mandate_proposal : received call for validating payment mandate for proposal_id {proposal_id}"
    )
    url = f"{MOTOR_ORCHESTRATOR_URL}/motororchestrator/internal/api/v1/renewals/proposal/validate-mandate-data"
    headers = {"x-app-name": "payment_lambda"}
    print("proposal_id as str", proposal_id)
    proposal_id = decrypt_id(proposal_id, 'auto_proposal')
    print("proposal_id as int", proposal_id)
    response = requests.get(url, params={"proposal_id": proposal_id}, headers=headers)

    logger.info(
        f"validate_mandate_proposal : got response from validating payment mandate for proposal_id {proposal_id}, response {response.text}"
    )
    if response.status_code != 200:
        return False
    return True


def validatePricingCallEligibility(data):
    try:
        notification_date = data.get('notification_date')
        date_format = "%Y-%m-%dT%H:%M:%SZ"  # Define the format
        # Convert string to datetime object
        notification_date = datetime.strptime(notification_date, date_format)
        if notification_date.date() == datetime.today().date():
            return True
        return False
    except Exception as e:
        return True


def trigger_r2d2(okind, ekind, oid, odata, edata, user_id=0):
    payload = {
        "oid": oid,
        "okind": okind,
        "ekind": ekind,
        "odata": odata,
        "edata": edata,
        "user_id": 0,
        "app": "Lambda",
    }

    logger.info(f"R2D2 API request --- {payload}")

    url = f"{R2D2_SERVICE_URL}/internal/api/r2d2/"
    response = requests.post(url, json=payload)

    if response.status_code == 200:
        logger.info("R2D2 API succeeded")
        return response.json()
    else:
        try:
            response_data = response.json()
        except ValueError:
            response_data = response.text
        raise Exception(
            "R2D2 API failed",
            response=response_data,
            status_code=response.status_code,
            payload=payload,
        )


def trigger_pricing_change_event(data, response):
    try:
        edata = {
            'proposal_ekey': data.get('schedule_reference_id', {}),
            'gross_premium': response.get('selected_plan').get('price').get('gross_premium'),
            'net_premium': response.get('selected_plan').get('price').get('net_premium'),
            'gst': response.get('selected_plan').get('price').get('gst').get('gst')
        }
        odata = {}
        ekind = "mandate_pricing_change"
        okind = "auto_proposal"
        trigger_r2d2(
            okind=okind,
            oid=response.get('schedule_reference_id', {}),
            ekind=ekind,
            odata=odata,
            edata=edata,
            user_id=None,
        )
    except Exception as e:
        logger.error(f"Exception occurred while triggering the event for mandate pricing change as {e}")
        return None


def get_schedule(payment_plan_id):
    url = f"{PAYMENT_URL}/api/v1/payment-plans/{payment_plan_id}"
    response = requests.get(url)
    return response.json()


def construct_payload(data, premium):
    instalement_id = data.get('installment_id')
    schedule_id = data.get('schedule_id')
    payment_plan_id = data.get('payments_plan_id')
    response = get_schedule(payment_plan_id)
    schedules = response.get('schedules')
    update_schedule = None
    for schedule in schedules:
        if schedule.get('schedule_id') == schedule_id:
            update_schedule = schedule
    previous_instalement_gross_amount = None
    new_instalement_gross_amount = None
    previous_instalement_net_amount = None
    new_instalement_net_amount = None
    new_instalement_net_gst = None
    previous_instalement_gst = None
    # Update all instalments
    for instalment in update_schedule["instalments"]:
        if instalement_id == instalment.get('instalment_id'):
            previous_instalement_gross_amount = instalment.get('gross_amount')
            previous_instalement_net_amount = instalment.get('break_up')['net_amount']
            previous_instalement_gst = instalment.get('break_up').get('tax_break_up')[0][
                'value']
            new_instalement_gross_amount = premium.get('gross_premium')
            new_instalement_net_amount = premium.get('net_premium')
            new_instalement_net_gst = premium.get('gst')
            instalment['gross_amount'] = new_instalement_gross_amount
            instalment.get('break_up')['net_amount'] = new_instalement_net_amount
            instalment.get('break_up').get('tax_break_up')[0]['value'] = new_instalement_net_gst
            break
    update_schedule['gross_amount'] = update_schedule[
                                          'gross_amount'] - previous_instalement_gross_amount + new_instalement_gross_amount
    update_schedule.get('break_up')['net_amount'] = update_schedule.get('break_up')[
                                                        'net_amount'] - previous_instalement_net_amount + new_instalement_net_amount
    update_schedule.get('break_up').get('tax_break_up')[0]['value'] = \
        update_schedule.get('break_up').get('tax_break_up')[0][
            'value'] - previous_instalement_gst + new_instalement_net_gst
    return {"schedules": [update_schedule]}


def updateProposalWithLatestPlan(data):
    try:
        proposal_ekey = data.get('schedule_reference_id')
        url = MOTOR_ORCHESTRATOR_URL + f"/motororchestrator/internal/api/v2/proposals/{proposal_ekey}/plans/details"
        param = {'proposal_ekey': proposal_ekey}
        response = requests.get(url, params=param)
        response = response.json()
        if response is not None and response.get('selected_plan') is not None:
            trigger_pricing_change_event(data, response)
            return {
                'gross_premium': response.get('selected_plan').get('price').get('gross_premium'),
                'net_premium': response.get('selected_plan').get('price').get('net_premium'),
                'gst': response.get('selected_plan').get('price').get('gst').get('gst')
            }

        return None
    except Exception as e:
        print(e)
        return None


def updateInstalementWithLatestPremium(data, premium):
    payment_plan_id = data.get('payments_plan_id')
    url = f"{PAYMENT_URL}/api/v1/payment-plans/{payment_plan_id}/update"
    payload = construct_payload(data, premium)
    print(payload)
    response = requests.post(url, json=payload)
    return response


def validatePaymentExecution(data):
    if data.get('type') == 'payment':
        return True
    return False


def payment_callback(data):
    try:
        URL = MOTOR_ORCHESTRATOR_URL + "/motororchestrator/api/v1/callbacks/payment/v2"
        response = requests.post(url=URL, json=data)
        print(data)
        print("response is " + response.text)
    except Exception as e:
        print(e)


def validate_notification_failure(data):
    payment_plan_id = data.get('payments_plan_id')
    schedule_id = data.get('schedule_id')
    instalment_id = data.get('installment_id')
    url = f"{PAYMENT_URL}/api/v1/internal/payment-plans/{payment_plan_id}/schedules/{schedule_id}/instalments/{instalment_id}/transaction-details"
    response = requests.get(url)
    response = response.json()
    if response.get('mandate_status') == 'notify_failed':
        return True
    return False


def validate_notification_failure_scenario(data):
    try:
        scheduled_date = data.get('scheduled_date')
        date_format = "%Y-%m-%dT%H:%M:%SZ"  # Define the format
        # Convert string to datetime object
        scheduled_date = datetime.strptime(scheduled_date, date_format)
        if scheduled_date.date() + timedelta(days=1) == datetime.today().date():
            if validate_notification_failure(data):
                return True
        return False
    except Exception as e:
        return True


def lambda_handler(events, context):
    try:
        print("lambda_handler : events received for payment_execution lambda_handler: ", events)
        print("lambda_handler : Records :", events.get("Records"))
        logger.info(events)
        print("world")
        records = events.get("Records")
        for record in records:
            print("hello")
            print("lambda_handler : record picked: ", record)
            print("lambda_handler : record body: ", record.get("body"))
            data = record.get("body")
            print("data :" + data)
            data = json.loads(data)
            print(type(data))
            if data.get('event_type') == 'mandate_reminder':
                cancelled_mandate(data)
            elif data.get('event_type') == 'payment_reminder':
                schedule_reference_id = data.get('schedule_reference_id')
                if validatePricingCallEligibility(data) and validate_mandate_proposal(
                        data.get('schedule_reference_id')):
                    premium = updateProposalWithLatestPlan(data)
                    if premium is None:
                        opt_out_mandate(schedule_reference_id)
                        continue
                    updateInstalementWithLatestPremium(data, premium)
                if validate_notification_failure_scenario(data):
                    opt_out_mandate(schedule_reference_id)
            elif validatePaymentExecution(data):
                payment_callback(data)
    except Exception as e:
        print(f"lambda_handler : error_while_processing_message  {e}")
        raise e
