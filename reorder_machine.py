import datetime
import http.client
import json
import ssl
import time
import streamlit as st
from dataclasses import dataclass
from secrets import token_hex
from typing import Optional

from colorama import init, Fore, Style
from tenacity import retry, retry_if_exception_type

addresses = {
    "test_location": "Moscow, Red Square, 1"
}


class Actions:
    ACCEPT = "accept"
    CANCEL = "cancel"
    CANCEL_FREE = "cancel_free"
    CANCEL_PAID = "cancel_paid"
    REORDER = "reorder"
    SAVE = "save"
    DUPLICATES = "duplicates"
    REVERSED = "reversed"
    REPORT = "report"
    SORTING_FILE = "sorting_file"
    FIND = "find"
    CLAIMS = "claims"
    SCANNED = "scanned"


@dataclass(unsafe_hash=True)
class Statuses:
    NEW = "new"
    ESTIMATING = "estimating"
    ESTIMATING_FAILED = "estimating_failed"
    READY_FOR_APPROVAL = "ready_for_approval"
    FAILED = "failed"
    ACCEPTED = "accepted"
    PERFORMER_LOOKUP = "performer_lookup"
    PERFORMER_DRAFT = "performer_draft"
    PERFORMER_FOUND = "performer_found"
    PERFORMER_NOT_FOUND = "performer_not_found"
    CANCELLED_BY_TAXI = "cancelled_by_taxi"
    PICKUP_ARRIVED = "pickup_arrived"
    READY_FOR_PICKUP_CONFIRMATION = "ready_for_pickup_confirmation"
    PICKUPED = "pickuped"
    DELIVERY_ARRIVED = "delivery_arrived"
    PAY_WAITING = "pay_waiting"
    READY_FOR_DELIVERY_CONFIRMATION = "ready_for_delivery_confirmation"
    DELIVERED = "delivered"
    DELIVERED_FINISH = "delivered_finish"
    RETURNING = "returning"
    RETURN_ARRIVED = "return_arrived"
    READY_FOR_RETURN_CONFIRMATION = "ready_for_return_confirmation"
    RETURNED_FINISH = "returned_finish"
    CANCELLED = "cancelled"
    CANCELLED_WITH_PAYMENT = "cancelled_with_payment"
    CANCELLED_WITH_ITEMS_ON_HANDS = "cancelled_with_items_on_hands"

    ROUTED = ["performer_draft", "performer_found", "pickup_arrived"]
    FINAL = ["estimating_failed", "failed", "performer_not_found", "cancelled_by_taxi", "delivered",
             "delivered_finish",
             "returned_finish", "cancelled", "cancelled_with_items_on_hands", "cancelled_with_payment"]
    FINAL_SUCCESS = ["delivered", "delivered_finish"]
    FINAL_RETURN = ["returned_finish"]

    @staticmethod
    def all_statuses():
        return [item[1] for item in Statuses.__dict__.items() if '_' not in item]


class Express:
    HOST = "b2b.taxi.yandex.net"
    GEOFIX_HOST = "api.delivery-sandbox.com"
    ROUTE = "/b2b/cargo/integration/v2/"
    ROUTE_V1 = "/b2b/cargo/integration/v1/"


class LogPlatform:
    HOST = "b2b-authproxy.taxi.yandex.net"
    GEOFIX_HOST = "api.delivery-sandbox.com"
    ROUTE = "/api/b2b/platform/"


class ReversedSettings:
    REVERSED_POINT = [32.029979, 34.797653]  # [latitude, longitude] for reorder_reversed action


class ReportSettings:
    STATUSES = ["delivered", "returning"]  # claims which received this status will be included in the report
    FOR_TODAY = True  # automatically generates report for today
    TIME_ZONES = {
        "Russia": 3,
        "Turkey": 3,
        "Israel": 2,
        "Serbia": 1,
        "Mexico": -6
    }


class SortingFile:
    MANUAL_CUTOFF = False
    STATUSES = Statuses.ROUTED


class Products:
    EXPRESS = "Express"
    LOG_PLATFORM = "LogPlatform"


class Settings:
    PRODUCT = Products.EXPRESS
    USE_GEOFIX = False  # will switch all requests to api.delivery-sandbox.com
    COUNTRY: Optional[str] = "Mexico"  # for GeoFix purposes, optional
    CITY: Optional[str] = "Mexico"  # for GeoFix purposes, optional
    CITY_COORDINATES: Optional[str] = [32.086827, 34.789577]  # [latitude, longitude], for GeoFix purposes


CLIENT_KEYS = st.secrets["CLIENT_KEYS"]
CLIENT_CLID = st.secrets["CLIENT_CLID"]

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

init(autoreset=True)

st.markdown(f"# Reorder machine")
st.error("First reorder claims, then click cancel to cancel old ones!")
client_name = st.selectbox("Select client", ["Petco", "Sanborns", "El Magico"], index=0)
orders_list = st.text_area("Claims to reorder", height=200, help="Copy and paste from the route reports app")
orders_list = orders_list.split()

client, token = CLIENT_CLID[client_name], CLIENT_KEYS[client_name]

host = Express.HOST
http_client = http.client.HTTPSConnection(host)


def make_request(endpoint, payload, method="POST", claim=""):
    headers = {
        'Accept-Language': 'en',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    http_client.request(method, f"{Express.ROUTE}{endpoint}", json.dumps(payload), headers)
    response = http_client.getresponse().read()
    # print(response)
    try:
        return json.loads(response) | {"claim_id": claim}
    except json.decoder.JSONDecodeError:
        return {"response": response}


def bulk_request(m: list[dict], c: list[str]):
    yield (make_request(claiming(method.get('method'), claim), method.get('payload'), claim=claim) for method in m
           for
           claim in c)


def claiming(source, predicate, lookup="{claim_id}"):
    return source.replace(lookup, predicate)


def handle_response(r: dict, f, check_claim=False):
    try:
        if check_claim:
            assert check_claim and 'claim_id' in r.keys(), f"{Fore.RED}No claim_id is specified{Fore.RESET}"
            assert r['claim_id'] != "", f"{Fore.RED}No claim_id is specified{Fore.RESET}"
        try:
            if 'code' in r.keys():
                st.error(f"{r['claim_id']} - {r['message']}")
            else:
                st.success(f"{f(r)}")
        except KeyError:
            st.error(f"{r['claim_id']} - {r}")
    except AssertionError as e:
        st.error(e)


def log(text, end=False):
    ending = '\n' if end else '\r'
    print(text + " " * 30, end=ending)


def find_claim(claim):
    claim_id = ""
    try:
        response = make_request("claims/search", {"external_order_id": claim, "limit": 5, "offset": 0})['claims']
        for claim_response in response:
            if len(claim_response['route_points']) <= 3:
                claim_id = claim_response['id']
    except (KeyError, IndexError):
        pass

    return claim_id


def find(interval="", pickup="", statuses=[], end_date="", date="", time_zone=0, duplicates=False, sorting=False):
    if len(statuses) == 0:
        statuses = ['']

    claims = []
    external_order_ids = []
    dups = []
    routes = [["barcode", "route_id"]]
    search = "/claims/search"
    for status in statuses:
        status_dict = {"status": status} if status != '' else {}
        results = make_request(search, {
            "limit": 500,
            "offset": 0,
        } | status_dict)
        while True:
            any_result = False
            if 'claims' not in results.keys():
                break
            for result in results['claims']:
                if interval != '':
                    if 'same_day_data' not in result.keys():
                        continue
                    if result['same_day_data']['delivery_interval']['from'] != interval:
                        continue
                if pickup != '':
                    if result['route_points'][0]['address']['fullname'] != pickup:
                        continue
                if end_date != '':
                    created = time.mktime(
                        datetime.datetime.strptime(result['created_ts'].split("T")[0], "%Y-%m-%d").timetuple())
                    end = time.mktime(datetime.datetime.strptime(end_date, "%Y-%m-%d").timetuple())
                    if created < end:
                        continue
                if date != '':
                    date_new = time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple())
                    start_ts = date_new + time_zone * 3600
                    end_ts = date_new + time_zone * 3600 + 24 * 3600
                    updated = time.mktime(datetime.datetime.strptime(result['updated_ts'].split(".")[0],
                                                                     "%Y-%m-%dT%H:%M:%S").timetuple())

                    if not (start_ts <= updated <= end_ts):
                        continue
                    else:
                        print(result['updated_ts'], result['status'])
                any_result = True
                if not duplicates:
                    print(Fore.GREEN + result['id'] + Fore.RESET)
                claims.append(result['id'])
                pickup_point = result['route_points'][0]

                if duplicates:
                    if 'external_order_id' in pickup_point.keys():
                        if pickup_point['external_order_id'] in external_order_ids:
                            dups.append(pickup_point['external_order_id'])
                            print(Fore.GREEN + pickup_point['external_order_id'] + Fore.RESET)

                        external_order_ids.append(pickup_point['external_order_id'])
                        external_order_ids = list(set(external_order_ids))

                if sorting:
                    if 'route_id' in result.keys():
                        routes.append([pickup_point['external_order_id'], result['route_id']])

            # if len(results['claims']) == 0:
            #     print(f"{Fore.YELLOW}The search was completed{Fore.RESET}")
            if not any_result:
                break
            if 'cursor' not in results.keys():
                break
            cursor = results['cursor']
            results = make_request(search, {
                "cursor": cursor
            })

        if sorting:
            print("Generating the sorting file")
    if len(claims) == 0:
        print(Fore.RED + "No claims were found" + Fore.RESET)
    return claims


claims = orders_list
col_reorder, col_cancel = st.columns(2)

with col_reorder:
    if st.button("Reorder", type="primary", use_container_width=True):
        sdd = "sdd"
        interval = {}
        created_claims = []
        for claim in claims:
            if len(claim) == 32:
                endpoint = f"claims/info?claim_id={claim}"
                payload = {}
            else:
                endpoint = f"claims/search"
                payload = {
                    "limit": 1,
                    "offset": 0,
                    "external_order_id": claim
                }
            claim_info = make_request(endpoint, payload)
            if 'claims' in claim_info.keys():
                if len(claim_info['claims']) > 0:
                    claim_info = claim_info['claims'][0]

            if interval == {} and sdd and 'same_day_data' not in interval.keys():
                # st.info(f"Getting information about starting point")
                start_point = claim_info['route_points'][0]['address']['coordinates']

                # st.info(f"Requesting Same-day nearest interval")
                delivery_methods = make_request("delivery-methods", {"start_point": start_point})

                if 'available_intervals' in delivery_methods['same_day_delivery'].keys() and len(
                        delivery_methods['same_day_delivery']['available_intervals']) != 0:
                    interval = delivery_methods['same_day_delivery']['available_intervals'][0]
                    st.info(f"Interval: {interval}")
                else:
                    st.error(f"No available intervals for this client")

            if sdd:
                claim_info['same_day_data'] = {"delivery_interval": interval}
                if 'client_requirements' in claim_info.keys():
                    del claim_info['client_requirements']

            if 'route_points' in claim_info.keys():
                for route_point, a in enumerate(claim_info['route_points']):
                    claim_info['route_points'][route_point]['point_id'] = \
                        claim_info['route_points'][route_point]['id']

            request_id = token_hex(16)
            f = lambda j: f"{j['id']}"
            create_response = make_request(f"claims/create?request_id={request_id}", claim_info)
            handle_response(create_response, f)
            if 'id' in create_response.keys():
                created_claims.append(create_response['id'])

        st.info(f"Approving claims:")
        if len(created_claims) < 50:
            time.sleep(3)

        # if len(created_claims) == 0:
        #     st.warning(f"Nothing to approve")
        for claim in created_claims:
            accept_response = make_request(f"claims/accept?claim_id={claim}", {"version": 1})
            f = lambda j: f"{j['id']} – accepted"
            handle_response(accept_response, f)

        claims = set(created_claims)


with col_cancel:
    if st.button("Cancel old orders", type="primary", use_container_width=True):
        methods = [
            {
                "method": "claims/cancel?claim_id={claim_id}",
                "payload": {"cancel_state": "free", "version": 1}
            },
            {
                "method": "claims/cancel?claim_id={claim_id}",
                "payload": {"cancel_state": "paid", "version": 1}
            }
        ]
        for claim in claims:
            if len(claim) != 32:
                claim_id = find_claim(claim)
                claim = claim if claim_id == '' else claim_id
            tries = 0
            for method in methods:
                try:
                    action_response = make_request(method['method'].replace("{claim_id}", claim),
                                                   method['payload'],
                                                   claim=claim)
                except http.client.RemoteDisconnected:
                    continue
                tries += 1
                if tries > 1 or 'status' in action_response.keys():
                    tries = 0
                    f = lambda j: f"{j['claim_id']} - {j['status']}"
                    handle_response(action_response, f, check_claim=True)
                if 'status' in action_response.keys():
                    break
