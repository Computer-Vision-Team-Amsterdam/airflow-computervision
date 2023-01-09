import os
from typing import Final, Callable

from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from common import OTAP_ENVIRONMENT


SLACK_CHANNEL: Final = os.environ.get("SLACK_CHANNEL")
SLACK_TOKEN: Final = os.environ.get("SLACK_WEBHOOK")


# Quadruple braces to escape f-string
MESSAGE_TEMPLATE =f"""
    *Environment* {OTAP_ENVIRONMENT}
    *Task*: {{{{ task.task_id }}}}
    *Dag*: {{{{ dag.dag_id }}}}
    *Execution Time*: {{{{ ts }}}}
    *Log Url*: {{{{ ti.log_url }}}}
    """

# def _message_from_context(message_header, context):
#     return f"""
#         {message_header}
#         *Environment* {OTAP_ENVIRONMENT}
#         *Task*: {context["task"].task_id}
#         *Dag*: {context["dag"].dag_id}
#         *Execution Time*: {context["ts"]}
#         *Log Url*: {context["ti"].log_url}
#         """

def get_prefab_slack_api_post_operator() -> SlackAPIPostOperator:
    return SlackAPIPostOperator(
        task_id="slack_status_message",
        token=SLACK_TOKEN,
        channel=SLACK_CHANNEL,
        text=MESSAGE_TEMPLATE,
    )


def _get_slack_callback(message_header: str) -> Callable[[], None]:
    operator = get_prefab_slack_api_post_operator()
    operator.text = f"""
        {message_header}
        {MESSAGE_TEMPLATE}
    """
    return operator.execute


def on_failure_callback() -> Callable[[], None]:
    return _get_slack_callback(":red_circle: DAG Failed.\n")


def on_retry_callback() -> Callable[[], None]:
    return _get_slack_callback(":warning: Retrying DAG.\n")


def on_success_callback() -> Callable[[], None]:
    return _get_slack_callback(":white_check_mark: DAG Succeeded.\n")
