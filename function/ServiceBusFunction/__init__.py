import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # Get connection to database
    connection = psycopg2.connect(  host="techconfdb-server.postgres.database.azure.com",
                                    dbname="techconfdb",
                                    user="azureuser@techconfdb-server",
                                    password="azurepass123$" )
    cursor = connection.cursor()
    
    try:
        # Get notification message and subject from database using the notification_id
        logging.info('Fetching notification message and subject...')
        notification_query = cursor.execute("SELECT message, subject FROM notification WHERE id = {};".format(notification_id))

        # Get attendees email and name
        logging.info('Fetching attendees email and name...')
        cursor.execute("SELECT first_name, last_name, email FROM attendee;")
        attendees = cursor.fetchall()

        # Loop through each attendee and send an email with a personalized subject
        logging.info('Sending emails...')
        for attendee in attendees:
            Mail('{}, {}, {}'.format({'john@doe.com'}, {attendee[2]}, {notification_query}))

        # Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        logging.info('Updating notifications...')
        notification_completed_date = datetime.utcnow()
        notification_status = 'Notified {} attendees'.format(len(attendees))
        update_query = cursor.execute("UPDATE notification SET status = '{}', completed_date = '{}' WHERE id = {};".format(notification_status, notification_completed_date, notification_id))
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        connection.rollback()
    finally:
        # Close connection
        cursor.close()
        connection.close()
