# define Python user-defined exceptions
class ApiKeyNotFoundException(Exception):
    pass

def exception_alert(channnel_id, function, exception):
    message = f'For this channel_id = {channnel_id}, at this function {function}, with exception {exception}'
    print(message)
    # In this we can integrate alert monitoring
    return message