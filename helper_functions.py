import configparser

def read_config_ini(file_path="config.ini"):
  """Reads a config.ini file and returns a dictionary of the settings.

  Args:
    file_path: The path to the config.ini file.

  Returns:
    A dictionary of the settings in the config.ini file.
  """

  config = configparser.ConfigParser()
  config.read(file_path)

  settings = {}
  for section in config.sections():
    for key, value in config.items(section):
      settings[f'{section}.{key}'] = value

  return settings