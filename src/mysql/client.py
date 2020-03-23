import json
from collections.abc import Iterable

from kbc.client_base import HttpClientBase

COMPANIES_DEFAULT_COLS = ["additionalDomains", "companyId", "isDeleted", "mergeAudits", "portalId", "stateChanges"]
COMPANY_DEFAULT_PROPERTIES = ['about_us', 'name', 'phone', 'facebook_company_page', 'city', 'country', 'website',
                              'industry', 'annualrevenue', 'linkedin_company_page',
                              'hs_lastmodifieddate', 'hubspot_owner_id', 'notes_last_updated', 'description',
                              'createdate', 'numberofemployees', 'hs_lead_status', 'founded_year',
                              'twitterhandle',
                              'linkedinbio']

DEAL_DEFAULT_COLS = ["associations.associatedCompanyIds",
                     "associations.associatedDealIds",
                     "associations.associatedVids",
                     "dealId",
                     "imports",
                     "isDeleted",
                     "portalId",
                     "properties.dealstage.source",
                     "properties.dealstage.sourceId",
                     "properties.dealstage.timestamp",
                     "properties.dealstage.value",
                     "properties.dealstage.versions",
                     "properties.hs_object_id.source",
                     "properties.hs_object_id.sourceId",
                     "properties.hs_object_id.timestamp",
                     "properties.hs_object_id.value",
                     "properties.hs_object_id.versions",
                     "stateChanges"]
DEAL_DEFAULT_PROPERTIES = ['authority', 'budget', 'campaign_source', 'hs_analytics_source', 'hs_campaign',
                           'hs_lastmodifieddate', 'need', 'timeframe', 'dealname', 'amount', 'closedate', 'pipeline',
                           'createdate', 'engagements_last_meeting_booked', 'dealtype', 'hs_createdate', 'description',
                           'start_date', 'closed_lost_reason', 'closed_won_reason', 'end_date', 'lead_owner',
                           'tech_owner', 'service_amount', 'contract_type',
                           'hubspot_owner_id',
                           'partner_name', 'notes_last_updated']

# endpoints

DEALS_ALL = 'deals/v1/deal/paged'
DEALS_RECENT = 'deals/v1/deal/recent/modified'

COMPANIES_ALL = 'companies/v2/companies/paged'
COMPANIES_RECENT = 'companies/v2/companies/recent/modified'

MAX_RETRIES = 10
BASE_URL = 'https://api.hubapi.com/'

COMPANY_PROPERTIES = 'properties/v1/companies/properties/'


class HubspotClient(HttpClientBase):
    """
    Basic HTTP client taking care of core HTTP communication with the API service.

    It exttends the kbc.client_base.HttpClientBase class, setting up the specifics for Hubspot service and adding
    methods for handling pagination.

    """

    def __init__(self, token):
        HttpClientBase.__init__(self, base_url=BASE_URL, max_retries=MAX_RETRIES, backoff_factor=0.3,
                                status_forcelist=(429, 500, 502, 504), default_params={"hapikey": token})

    def _get_paged_result_pages(self, endpoint, parameters, res_obj_name, limit_attr, offset_req_attr, offset_resp_attr,
                                has_more_attr, offset, limit):
        """
        Generic pagination getter method returning Iterable instance that can be used in for loops.

        :param endpoint:
        :param parameters:
        :param res_obj_name:
        :param limit_attr:
        :param offset_req_attr:
        :param offset_resp_attr:
        :param has_more_attr:
        :param offset:
        :param limit:
        :return:
        """
        has_more = True
        while has_more:

            parameters[offset_req_attr] = offset
            parameters[limit_attr] = limit

            req = self.get_raw(self.base_url + endpoint, params=parameters)
            resp_text = str.encode(req.text, 'utf-8')
            req_response = json.loads(resp_text)

            if req_response[has_more_attr]:
                has_more = True
            else:
                has_more = False
            offset = req_response[offset_resp_attr]

            yield req_response[res_obj_name]

    def get_companies(self, recent=False, fields=None):

        offset = 0
        if not fields:
            company_properties = COMPANY_DEFAULT_PROPERTIES
        else:
            company_properties = fields

        parameters = {'properties': company_properties}

        if recent:
            return self._get_paged_result_pages(COMPANIES_RECENT, parameters, 'results', 'count', 'offset', 'offset',
                                                'hasMore', offset, 200)
        else:
            return self._get_paged_result_pages(COMPANIES_ALL, parameters, 'companies', 'limit', 'offset', 'offset',
                                                'has-more', offset, 250)

    def get_company_properties(self):
        req = self.get_raw(self.base_url + COMPANY_PROPERTIES)
        req_response = req.json()
        return req_response

    def get_deals(self, start_time=None, fields=None) -> Iterable:
        """
        Get either all available deals or recent ones specified by start_time.

        API supports more options, possible to extend in the future
        :type fields: list list of deal properties to get
        :param start_time: datetime
        :return: generator object with all available pages
        """
        offset = 0
        if not fields:
            deal_properties = DEAL_DEFAULT_PROPERTIES
        else:
            deal_properties = fields

        parameters = {'properties': deal_properties, 'propertiesWithHistory': 'dealstage',
                      'includeAssociations': 'true'}
        if start_time:
            parameters['since'] = int(start_time.timestamp() * 1000)
            return self._get_paged_result_pages(DEALS_RECENT, parameters, 'results', 'count', 'offset', 'offset',
                                                'hasMore',
                                                offset, 100)
        else:
            return self._get_paged_result_pages(DEALS_ALL, parameters, 'deals', 'limit', 'offset', 'offset', 'hasMore',
                                                offset, 250)
