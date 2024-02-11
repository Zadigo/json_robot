import pathlib
import dotenv


PROJECT_PATH = pathlib.Path(__file__).parent.absolute()

dotenv.load_dotenv(PROJECT_PATH / '.env')
