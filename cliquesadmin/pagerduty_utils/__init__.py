import pygerduty
import sys
import traceback


def stacktrace_to_pd_event(subdomain, api_key, service_key):
    exc_type, exc_value, exc_tb = sys.exc_info()
    stack = traceback.format_exception(exc_type, exc_value, exc_tb)
    stack = ''.join(stack).format()
    pager = pygerduty.PagerDuty(subdomain, api_key)
    pager.trigger_incident(service_key, stack)