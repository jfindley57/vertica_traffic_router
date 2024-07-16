from datadog import initialize, api
import logging

logging.getLogger('datadog').setLevel(logging.CRITICAL)

# Update below
options = {
        "api_key": "{api_key}",
        "app_key": "{app_key}",
        "api_host": "https://datadog.monitoring.{account}.com" # Replace account
    }

initialize(**options)


def send_values(usage, tag, metric):
    api.Metric.send(type="count", metric=metric, points=int(usage), tags=tag)


def send_usage(usage, tag, metric):
    api.Metric.send(type="gauge", metric=metric, points=float(usage), tags=tag)


def send_event(title, text, tags, alert_type='info'):
    api.Event.create(title=title, text=text, tags=tags, alert_type=alert_type)


if __name__ == '__main__':
    send_event('Proxy Starting', '', 'ec-proxy01.cl.test.com')
