using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace OSISoftCompletionData
{
    public class PIWebAPIClient
    {
        private HttpClient client;

        /* Initiating HttpClient using the default credentials. 
         * This can be used with Kerberos authentication for PI Web API. */
        public PIWebAPIClient()
        {
            client = new HttpClient(new HttpClientHandler() { UseDefaultCredentials = true });
        }

        /* Initializing HttpClient by providing a username and password. The basic authentication header is added to the HttpClient. 
         * This can be used with Basic authentication for PI Web API. */
        public PIWebAPIClient(string userName, string password)
        {
            client = new HttpClient();
            string authInfo = Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(String.Format("{0}:{1}", userName, password)));
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", authInfo);
        }

        /* Disposing the HttpClient. */
        public void Dispose()
        {
            client.Dispose();
        }

        /* Async GET request. This method makes a HTTP GET request to the uri provided 
        * and throws an exception if the response does not indicate a success. */
        public async Task<JObject> GetAsync(string uri)
        {
            HttpResponseMessage response = await client.GetAsync(uri);
            string content = await response.Content.ReadAsStringAsync();
            if (!response.IsSuccessStatusCode)
            {
                var responseMessage = "Response status code does not indicate success: " + (int)response.StatusCode + " (" + response.StatusCode + " ). ";
                throw new HttpRequestException(responseMessage + Environment.NewLine + content);
            }
            return JObject.Parse(content);
        }

        /* Calling the GetAsync method and waiting for the results. */
        public JObject GetRequest(string url)
        {
            Task<JObject> t = this.GetAsync(url);
            t.Wait();
            return t.Result;
        }
    }
}
