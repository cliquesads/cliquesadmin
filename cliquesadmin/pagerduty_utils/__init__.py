import pygerduty
import sys
import traceback

def create_pd_event_wrapper(subdomain, api_key, service_key):
    def create_pd_event(msg):
        msg = msg[:1024]
        pager = pygerduty.PagerDuty(subdomain, api_key)
        pager.trigger_incident(service_key, msg)
    return create_pd_event

def stacktrace_to_pd_event(subdomain, api_key, service_key):
    exc_type, exc_value, exc_tb = sys.exc_info()
    stack = traceback.format_exception(exc_type, exc_value, exc_tb)
    stack = ''.join(stack).format()
    pager = pygerduty.PagerDuty(subdomain, api_key)
    stack = stack[:1024]
    pager.trigger_incident(service_key, stack)

