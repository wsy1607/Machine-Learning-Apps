#this script initializes the beer club project, don't modify this file without any significant changes

from __future__ import absolute_import

from celery import Celery, Task

app = Celery('emailCampaign',
             broker='amqp://guest@localhost//',
             include=['emailCampaign.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
                # Sent backend to 'amqp'
                CELERY_RESULT_BACKEND = 'amqp',
                # Expect all tasks to be communicated in `json`
                CELERY_RESULT_SERIALIZER = 'json',
                CELERY_TASK_SERIALIZER = 'json',
                CELERY_ACCEPT_CONTENT = ['json'],
)

if __name__ == '__main__':
    app.start()
