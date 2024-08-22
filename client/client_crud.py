
from client_ import make_query,delete_document,insert_document,update_document,download_file
from logguer import log_message

log_message(make_query('tyrio',[]))
#log_message(delete_document('tyrion'))
log_message(insert_document("tyrion.txt",'tyrion y valentina'))

log_message(download_file("tyrion.txt"))
log_message('Realizado')

