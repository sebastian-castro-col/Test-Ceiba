from config import get_credentials
from transformations.customers import generate_transform_clients


def main():
    print("=" * 60)
    print("LOAD FINTRUST DATA INTO BIGQUERY")
    print("=" * 60)

    credentials = get_credentials()
    print(f"Secret loaded successfully with keys: {', '.join(sorted(credentials.keys()))}")

    generate_transform_clients()

    print("=" * 60)
    print("PROCESO FINALIZADO CORRECTAMENTE")
    print("=" * 60)


if __name__ == "__main__":
    main()
