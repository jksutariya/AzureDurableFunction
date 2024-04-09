using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using static Microsoft.AspNetCore.Hosting.Internal.HostingApplication;

namespace FunctionApp1
{
    public static class AzureTaskFunction
    {

        [FunctionName("ProcessTransaction")]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            log.LogInformation($"Start");

            string jsonString = context.GetInput<object>().ToString();
            log.LogInformation(jsonString);
            //string jsonString = @"{
            //    ""correlationId"": ""0EC1D320-3FDD-43A0-84B8-3CF8972CDCD8"",
            //    ""tenantId"": ""345"",
            //    ""transactionId"": ""eyJpZCI6ImE2NDUzYTZlLTk1NjYtNDFmOC05ZjAzLTg3ZDVmMWQ3YTgxNSIsImlzIjoiU3RhcmxpbmciLCJydCI6InBheW1lbnQifQ"",
            //    ""transactionDate"": ""2024-02-15 11:36:22"",
            //    ""direction"": ""Credit"",
            //    ""amount"": ""345.87"",
            //    ""currency"": ""EUR"",
            //    ""description"": ""Mr C A Woods"",
            //    ""sourceaccount"": {
            //        ""accountno"": ""44421232"",
            //        ""sortcode"": ""30-23-20"",
            //        ""countrycode"": ""GBR""
            //    },
            //    ""destinationaccount"": {
            //        ""accountno"": ""87285552"",
            //        ""sortcode"": ""10-33-12"",
            //        ""countrycode"": ""HKG""
            //    }
            //}";

            JObject eventData = JObject.Parse(jsonString);



            // Retrieve Tenant settings
            var tenantId = eventData["tenantId"].ToString();
            var tenantSettings = await context.CallActivityAsync<JObject>("GetTenantSettings", tenantId);
            log.LogInformation($"Start {tenantId}");
            try
            {
                // Assess the transaction against the Tenant settings
                var violation = AssessTransaction(eventData, tenantSettings);

                if (violation != null)
                {
                    // Raise alert and send payment to holding queue
                    await context.CallActivityAsync("RaiseAlert", violation);
                    return violation.ToString();
                }

                // Send payment to processing queue
                await context.CallActivityAsync("SendToProcessingQueue", eventData);
                
            }
            catch (Exception ex)
            {
                // Log exception and send message to operations topic
                log.LogError($"Exception occurred: {ex.Message}");
                await context.CallActivityAsync("SendToOperationsTopic", ex.Message);
            }
            return eventData.ToString();
        }

        [FunctionName("GetTenantSettings")]
        public static async Task<JObject> GetTenantSettings([ActivityTrigger] string tenantId)
        {
            string jsonString = @"{""tenantsettings"": [{""tenantid"": ""345"",""velocitylimits"": {""daily"": ""2500""},""thresholds"": {""pertransaction"": ""1500""},""countrysanctions"": {""sourcecountrycode"": ""AFG, BLR, BIH, IRQ, KEN, RUS"",""destinationcountrycode"": ""AFG, BLR, BIH, IRQ, KEN, RUS, TKM, UGA""}}]}";

            JObject tenantSettings = JObject.Parse(jsonString);

            return await Task.FromResult(tenantSettings);
        }

        [FunctionName("RaiseAlert")]
        public static Task RaiseAlert([ActivityTrigger] JObject violation, ILogger log)
        {
            // Log the violation details
            log.LogWarning($"Potential fraudulent activity detected: {violation}");

            return Task.CompletedTask;
        }

        [FunctionName("SendToProcessingQueue")]
        public static async Task SendToProcessingQueue(
            [ActivityTrigger] JObject eventData,
            ILogger log)
        {
            try
            {
                string serviceBusConnectionString = Environment.GetEnvironmentVariable("ServiceBusConnectionString");

                // Parse the message content
                string messageContent = eventData.ToString();

                // Create a queue client
                QueueClient queueClient = new QueueClient(serviceBusConnectionString, "myqueues");

                // Create a message and add it to the queue
                var message = new Message(Encoding.UTF8.GetBytes(messageContent));

                await queueClient.SendAsync(message);

                await queueClient.CloseAsync();
                //await SendToOperationsTopic("Error",log);
                log.LogInformation($"Message added to processing queue: {messageContent}");
            }
            catch (Exception ex)
            {
                // Log exception and send message to operations topic
                log.LogError($"Exception occurred while sending to processing queue: {ex.Message}");
                throw; // Rethrow exception to be caught by orchestrator
            }
            // Retrieve the connection string for the Service Bus namespace

        }

        [FunctionName("SendToOperationsTopic")]
        public static async Task SendToOperationsTopic(
            [ActivityTrigger] string errorMessage,
            ILogger log)
        {
            try
            {
                string serviceBusConnectionString = Environment.GetEnvironmentVariable("ServiceBusConnectionString");

                TopicClient topicClient = new TopicClient(serviceBusConnectionString, "myqueues");

                // Create a message using the message body
                var message = new Message(Encoding.UTF8.GetBytes(errorMessage));

                // Send the message to the topic
                await topicClient.SendAsync(message);
                log.LogInformation($"Sending message to operations topic: {errorMessage}");
                // Send message to operations topic code here
            }
            catch (Exception ex)
            {
                // Log exception occurred while sending to operations topic
                log.LogError($"Exception occurred while sending to operations topic: {ex.Message}");
            }
        }

        // Helper function to assess transaction against Tenant settings
        private static JObject AssessTransaction(JObject eventData, JObject jq)
        {
            string direction = eventData["direction"].ToString();
            decimal amount = decimal.Parse(eventData["amount"].ToString());

            JArray tenantSettingsArray = (JArray)jq["tenantsettings"];
            JObject tenantSettings = (JObject)tenantSettingsArray[0];
            // Retrieve tenant-specific settings
            decimal dailyVelocityLimit = decimal.Parse(tenantSettings["velocitylimits"]["daily"].ToString());
            decimal perTransactionThreshold = decimal.Parse(tenantSettings["thresholds"]["pertransaction"].ToString());
            string sourceCountryCode = tenantSettings["countrysanctions"]["sourcecountrycode"].ToString();
            string destinationCountryCode = tenantSettings["countrysanctions"]["destinationcountrycode"].ToString();

            // Perform assessment based on the parsed data and tenant settings
            if (amount > perTransactionThreshold)
            {
                // Transaction amount exceeds the per-transaction threshold
                return JObject.FromObject(new
                {
                    violationType = "ThresholdExceeded",
                    message = $"Transaction amount exceeds the per-transaction threshold: {amount}"
                });
            }

            // Return null if no violations detected
            return null;
        }


        [FunctionName("RunHttp")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post",Route = "{tentId:string}")] HttpRequestMessage req,string tentId,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            var data = await req.Content.ReadAsAsync<object>();
            string instanceId = await starter.StartNewAsync("ProcessTransaction", data);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

    }
}