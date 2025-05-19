import pika
import json
from colorama import Fore, Style, init

class RabbitmqConsumer:
    def __init__(self, callback) -> None:
        self.__host = "localhost"
        self.__port = 5672
        self.__username = "user"
        self.__password = "password"
        self.__queue = "fila_empresas"
        self.__callback = callback
        self.__channel = self.__create_channel()

    def __create_channel(self):
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__password
            )
        )

        channel = pika.BlockingConnection(connection_parameters).channel()
        channel.queue_declare(
            queue=self.__queue,
            durable=True
        )
        channel.basic_consume(
            queue=self.__queue,
            auto_ack=False,
            on_message_callback=self.__callback
        )

        return channel
    
    def start(self):
        print(f'Listen RabbitMQ on Port 5672')
        self.__channel.start_consuming()

def formatar_empresa(data):
    print(f"\n{Fore.GREEN}{'='*60}")
    print(f"{Fore.CYAN}📦 Empresa: {Style.BRIGHT}{data.get('razao_social', 'N/A')}")
    print(f"{Fore.YELLOW}CNPJ Básico: {data.get('cnpj_basico', 'N/A')}")
    print(f"{Fore.YELLOW}Natureza Jurídica: {data.get('natureza_juridica', 'N/A')}")
    print(f"{Fore.YELLOW}Porte: {data.get('porte_empresa', 'N/A')}")

    # Simples Nacional
    simples = data.get('simples', {})
    print(f"{Fore.MAGENTA}Simples Nacional: {Style.BRIGHT}{'SIM' if simples.get('opcao_simples') else 'NÃO'}")
    if simples.get('opcao_simples'):
        print(f"  📅 Desde: {simples.get('data_opcao_simples', 'N/A')}")
    print(f"{Fore.MAGENTA}MEI: {Style.BRIGHT}{'SIM' if simples.get('opcao_mei') else 'NÃO'}")

    # Sócios
    print(f"{Fore.CYAN}\n👥 Sócios:")
    for socio in data.get('socios', []):
        print(f" - {socio.get('nome_socio')} ({socio.get('qualificacao_socio')}) - {socio.get('faixa_etaria')}")

    # Estabelecimentos
    print(f"{Fore.BLUE}\n🏢 Estabelecimentos:")
    for est in data.get('estabelecimentos', []):
        print(f" • {Style.BRIGHT}{est.get('nome_fantasia', 'N/A')} ({est.get('inscricao_federal')})")
        print(f"   {est.get('tipo_logradouro', '')} {est.get('logradouro', '')}, {est.get('numero', '')} - {est.get('bairro', '')}")
        print(f"   {est.get('municipio', '')} - {est.get('uf', '')} | CEP: {est.get('cep', '')}")
        print(f"   Situação: {est.get('situacao_cadastral', 'N/A')} desde {est.get('data_situacao', 'N/A')}")
        print(f"   E-mail: {est.get('email', 'N/A')} | Tel: {est.get('telefone1', 'N/A')}")

        # CNAEs
        print(f"   🏷️ CNAEs:")
        for cnae in est.get('cnaes', []):
            tipo = "PRINCIPAL" if cnae.get('principal') else "Secundário"
            print(f"     - [{tipo}] {cnae.get('codigo')}: {cnae.get('descricao')}")

    print(f"{Fore.GREEN}{'='*60}\n")        

def minha_callback(ch, method, properties, body):
    try:
        empresa = json.loads(body.decode('utf-8'))
        formatar_empresa(empresa)
    except Exception as e:
        print(f"{Fore.RED}❌ Erro ao processar mensagem: {e}")
    ch.basic_ack(delivery_tag=method.delivery_tag)



rabitmq_consumer = RabbitmqConsumer(minha_callback)
rabitmq_consumer.start()