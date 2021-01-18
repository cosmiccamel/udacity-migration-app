import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    try:
        notification_id = int(msg.get_body().decode('utf-8'))
        logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)
    except Exception as err:
        logging.error("kaput")
        logging.error(err)
    
    try:
        connection = psycopg2.connect(  host="techconfdb-server.postgres.database.azure.com",
                                        dbname="techconfdb",
                                        user="azureuser@techconfdb-server",
                                        password="azurepass123$" )
        cursor = connection.cursor()    
    except Exception as err:
        logging.error("kaput")
        logging.error(err)

    try:
        
        # Get notification message and subject from database using the notifiation_id
        logging.info('Get message and subject')
        cursor.execute("Select message, subject from notification where id= {} ;".format(notification_id))
        note_query = cursor.fetchone()

        # Get attendees email and name
        logging.info('Get name and email')
        cursor.execute("SELECT first_name, last_name, email FROM attendee;")
        conf_alien = cursor.fetchall()

        # Loop through each attendee and send an email with a personalized subject
        logging.info('Sending email to attendee ')
        for human in conf_alien:
            Mail('{}, {}, {}'.format({'azureuser@techconfdb-server'}, {human[2]}, {note_query}))

        # Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        end_date = datetime.utcnow()
        logging.info('Update table')

        
        msg_status = 'Notified {} attendees'.format(len(conf_alien))
        
        text_msg = "UPDATE notification SET status = '{}', completed_date = '{}' WHERE id = {};".format(msg_status, end_date, notification_id)
        update_query = cursor.execute(text_msg)
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        connection.rollback()
    finally:
        # Close connection
        cursor.close()
        connection.close()
