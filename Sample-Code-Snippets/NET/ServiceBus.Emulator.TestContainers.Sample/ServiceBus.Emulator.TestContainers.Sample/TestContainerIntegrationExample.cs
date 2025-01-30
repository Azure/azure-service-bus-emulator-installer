using Azure.Messaging.ServiceBus;
using Testcontainers.ServiceBus;

namespace ServiceBus.Emulator.TestContainers.Sample
{
    [TestFixture]
    public class TestContainerIntegrationExample
    {
        private ServiceBusContainer _serviceBusEmulatorContainer;
        private readonly string _testQueue = "contoso-queue";

        [OneTimeSetUp]
        public async Task Setup()
        {
            //Sample Emulator Config; can be generated dynamically based on the test requirements, complex configs alternatively can also be read from a file
            string emulatorConfig = $@"{{
            ""UserConfig"": {{
                ""Namespaces"": [
                    {{
                        ""Name"": ""sbemulatorns"",
                        ""Queues"": [
                            {{
                                ""Name"": ""{_testQueue}"",
                            }}
                        ]
                    }}
                ],
                ""Logging"": {{
                    ""Type"":""File""
                             }}
                }}
            }}";

            //Write the emulator config to a temp file to be mounted in the container as Config.json 
            var emulatorConfigFilePath = Path.GetTempFileName();
            File.WriteAllText(emulatorConfigFilePath, emulatorConfig);

            //When running the following tests you are accepting SB Emulator EULA : https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt
            _serviceBusEmulatorContainer = new ServiceBusBuilder()
            .WithBindMount(emulatorConfigFilePath, "/ServiceBus_Emulator/ConfigFiles/Config.json") 
            .WithAcceptLicenseAgreement(true)
            .Build();

            await _serviceBusEmulatorContainer.StartAsync();
        }

        [Test]
        public async Task SendAndReceiveTest()
        {
            //Arrange
            const string helloMessage = "Hello Service-Bus !";
            var message = new ServiceBusMessage(helloMessage);
            await using var client = new ServiceBusClient(_serviceBusEmulatorContainer.GetConnectionString());

            //Act
            var sender = client.CreateSender(_testQueue);
            await sender.SendMessageAsync(message);

            //Assert that the sent message is enqueued 
            var receiver = client.CreateReceiver(_testQueue);
            var receivedMessage = await receiver.ReceiveMessageAsync();
            Assert.That(receivedMessage.Body.ToString(), Is.EqualTo(helloMessage));
        }

        [OneTimeTearDown]
        public async Task Teardown()
        {
            await _serviceBusEmulatorContainer.StopAsync();
            await _serviceBusEmulatorContainer.DisposeAsync();
        }
    }
}