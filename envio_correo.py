from email.message import EmailMessage
import smtplib

def envio_correo(remitente,clave,destinatario,asunto,mensaje):
    try:
        email = EmailMessage()
        email["From"] = remitente
        email["To"] = destinatario
        email["Subject"] = asunto
        email.set_content(mensaje)
        smtp = smtplib.SMTP_SSL('smtp.gmail.com')
        smtp.login(remitente, clave)
        smtp.sendmail(remitente,destinatario,email.as_string())
        return True
        
    except smtplib.SMTPException as err:
        return False

    finally:
        smtp.quit()