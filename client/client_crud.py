
from client_ import make_query,delete_document,insert_document,update_document,download_file
from logguer import log_message
log_message(insert_document(f"tyrion.txt","Tyrion es patriota"))
#log_message(make_query('txt',[]))
#log_message(delete_document('tyrion'))
#log_message(insert_document("primero.txt",'primer documento'))
#log_message(update_document("segundo",'segundo documento modificado'))
#log_message(insert_document("tercero.txt",'tercer documento'))
#log_message(insert_document("cuarto.pdf",'primer documento pdf cuarto en total'))
#log_message(insert_document("quinto.pdf",'quinto documento'))
#log_message(update_document("t.txt",'tyrion y valentina'))
#log_message(download_file("tyrion.txt"))
log_message(download_file("tyrion.txt"))
log_message('Realizado')

