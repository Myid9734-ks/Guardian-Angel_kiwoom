import logging 

logging.basicConfig(
    format='%(asctime)s : %(levelname)-10s : %(message)s',
    filename="./example.log",
    # filename="./log/example.log",
    filemode='a', 
    level=logging.DEBUG # 대문자 
)

# logging.debug('This is a debug message')
# logging.info('This is an info message')
# logging.warning('This is a warning message')
# logging.error('This is an error message')
# logging.critical('This is a critical message')




try:
    prin1t('a')
except Exception as e:
    logging.exception("Exception occurred", exc_info=True)
# logging.debug("This is debug")