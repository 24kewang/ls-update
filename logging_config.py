import logging

def setup_loggers():
    # Create logger for asset_update module
    asset_logger = logging.getLogger('asset_update')
    asset_logger.setLevel(logging.INFO)
    
    # Create file handler and formatter for asset_update
    asset_handler = logging.FileHandler('asset_update_audit.log')
    asset_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    asset_handler.setFormatter(asset_formatter)
    asset_logger.addHandler(asset_handler)
    
    # Create logger for test module
    test_logger = logging.getLogger('test')
    test_logger.setLevel(logging.DEBUG)
    
    # Create file handler and formatter for test
    test_handler = logging.FileHandler('debug.log')
    test_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    test_handler.setFormatter(test_formatter)
    test_logger.addHandler(test_handler)
    
    # Optionally add a StreamHandler to both for console output
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(asset_formatter)  # or test_formatter
    asset_logger.addHandler(stream_handler)
    test_logger.addHandler(stream_handler)
