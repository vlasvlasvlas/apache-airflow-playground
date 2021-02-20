# https://github.com/vishalkashyap95/Data_Engineering_Learnings/blob/487453e56fc0e0bc836712bbd125bce57fe76673/Week_3_Task/Download_Attachment.py

import os
from imbox import Imbox ## https://pypi.org/project/imbox/
#from cryptography.fernet import Fernet # https://www.mssqltips.com/sqlservertip/5173/encrypting-passwords-for-use-with-python-and-sql-server/#:~:text=Install%20Cryptography%20Library%20and%20Create%20Key&text=The%20first%20thing%20we're,then%20we'll%20use%20it.
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,date
from airflow import DAG

def download_csv_from_email():

	host = "imap.gmail.com"
	username = "gmail"
	password = "password"

	destination_folder = '/tmp'

	# if not os.path.isdir(destination_folder):
	#     os.makedirs(destination_folder,exist_ok=True)

	mail = Imbox(host, username = username,password = password, ssl = True, ssl_context = None, starttls = False)
	all_inbox_messages = mail.messages(folder='Inbox', sent_from ='gmail',unread=True,raw='has:attachment')

	if len(all_inbox_messages)>0:
		for (uid,message) in all_inbox_messages:
			print("Subject of the email :---> ",message.subject)
			if message.subject.lower().__contains__("datatest"):
				# mail.mark_seen(uid)
				for attachment in message.attachments:
					try:
						if attachment.get('filename').endswith('.txt'):
							print('Attachments :--->',attachment.get('filename'))
							full_file_path = "{0}/{1}".format(destination_folder,attachment.get('filename'))
							print("full file path :----> ",full_file_path)
							with open(full_file_path,'wb') as fw:
								fw.write(attachment.get('content').read()) 
					except Exception as e:
						print("Exception caught while downloading the attachment :--> ",e)
					finally:
						print("Inside finally block..Code executed.")

			mail.mark_seen(uid)
	else:
		print("No new unread message in Inbox which has attachments.")


default_args = {
	'owner':'mpch',
	'start_date': date.today().strftime("%Y-%m-%d")
}


dag = DAG(dag_id='gmail_attach',
		default_args=default_args,
        schedule_interval= None #schedule_interval= '* * * * *'
	)
		
task_to_download_csv_from_email = PythonOperator(task_id='task_to_download_csv_from_email',python_callable=download_csv_from_email,dag=dag)