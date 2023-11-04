import secrets
import string
import hjson
import argparse
import os
from librescada_utils.logger import logger

"""
    This module generates credentials for the services and users defined in the configuration file `--conf_file`.
    The credentials are stored in the output directory `--output_dir` in the following format:
    - global txt credentials file for each service
    - individual txt files for each service and user
    - global environment file for all services
    
    Expected configuration file format:
    {
      "n_characters_in_password": 20,
    
      "services":{
    
        "opc_server": {
          "prefix": "UA",
          "users":{
              "ADMIN": "admin",
              "USER": "user",
              "OPERATOR": "operator",
              "DATALOGGER": "data_logger"
          }
        }
    
        "database": {
          "prefix": "DB",
          "users":{
              "ADMIN": "admin",
              "USER": "user",
          }
        }
        
      }
    }
    
    Example:
    python credentials_generator.py --conf_file configuration_files/credentials.hjson --output_dir .secrets
"""

argparser = argparse.ArgumentParser()
argparser.add_argument('-c', '--conf_file', help='Path to the configuration file',
                       required=False, default='configuration_files/credentials.hjson')
argparser.add_argument('-o', '--output_dir', help='Path to the output directory',
                       required=False, default='.secrets')
args = argparser.parse_args()

with open(args.conf_file) as f:
    config = hjson.load(f)

alphabet = string.ascii_letters + string.digits

def generate_password(n=20):
    return ''.join(secrets.choice(alphabet) for _ in range(n))


def main():
    # Create directories for storing output files if it does not exist
    out_dir= args.output_dir
    os.makedirs(out_dir, exist_ok=True)

    # Create global environment file
    with open(os.path.join(out_dir, "env"), "w") as f:
        for key, data in config["services"].items():
            users = data["users"]
            prefix = data["prefix"]
            for user_key, username in users.items():
                f.write(f"{prefix}_{user_key}_USERNAME={username}\n")

    logger.info(f"Created global environment file at {os.path.join(out_dir, 'env')}")

    # Create:
    # - global txt credentials file for each service
    # - individual txt files for each service and user

    n = config.get("n_characters_in_password", 20)

    # Iterate through the configuration
    for key, data in config["services"].items():
        prefix = data["prefix"]
        users = data["users"]

        # Create credentials file
        credentials_file = f"credentials_{key.lower()}.txt"
        with open(os.path.join(out_dir, credentials_file), "w") as f:
            for user_key, user_value in users.items():
                password_value = generate_password(n=n)

                if isinstance(user_value, dict):
                    username_value = user_value["username"]
                    role_value = user_value.get("role", None)
                else:
                    username_value = user_value
                    role_value = None

                username = f"{prefix}_{user_key}_USERNAME={username_value}\n"
                password = f"{prefix}_{user_key}_PASSWORD={password_value}\n"
                role = f"{prefix}_{user_key}_ROLE={role_value}\n" if role_value else None

                # Write to credentials file
                f.write(username)
                f.write(password)
                if role is not None:
                    f.write(role)

                # Open and write to individual password file
                password_file = f"{prefix}_{user_key}_PASSWORD.txt"
                with open(os.path.join(out_dir, password_file), "w") as f2:
                    f2.write(password_value)

                logger.info(f'Created service {key} user {user_key} password file at {os.path.join(out_dir, password_file)}')

        logger.info(f'Created service {key} credentials file at {os.path.join(out_dir, credentials_file)}')


if __name__ == '__main__':
    main()