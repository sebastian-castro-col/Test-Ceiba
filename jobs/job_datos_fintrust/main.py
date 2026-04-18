from config import get_credentials
from transformations.customers import generate_transform_customers
from transformations.loans import generate_transform_loans
from transformations.installments import generate_transform_installments
from transformations.payments import generate_transform_payments
from analytics.cartera import generate_analytics_cartera
from views.bi_dashboard import generate_view_dashboard_bi


def main():
    print("=" * 60)
    print("LOAD FINTRUST DATA INTO BIGQUERY")
    print("=" * 60)

    credentials = get_credentials()
    if isinstance(credentials, dict):
        print(f"Secret loaded successfully with keys: {', '.join(sorted(credentials.keys()))}")
    else:
        print("Secret loaded successfully as plain text.")

    print("=" * 60)
    print("PROCESO DE LIMPIEZA Y TRANSFORMACION INICIADO CORRECTAMENTE")
    print("=" * 60)

    generate_transform_customers()
    generate_transform_loans()
    generate_transform_installments()
    generate_transform_payments()

    print("=" * 60)
    print("PROCESO DE ANALISIS INICIADO CORRECTAMENTE")
    print("=" * 60)

    generate_analytics_cartera()

    print("=" * 60)
    print("GENERACION DE VISTA FINAL DASHBOARD BI")
    print("=" * 60)
    
    generate_view_dashboard_bi()

    print("=" * 60)
    print("PROCESO FINALIZADO CORRECTAMENTE")
    print("=" * 60)


if __name__ == "__main__":
    main()
