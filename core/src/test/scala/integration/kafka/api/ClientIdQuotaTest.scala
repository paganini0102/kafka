/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package kafka.api

import java.util.Properties

import kafka.server.{DynamicConfig, KafkaConfig, QuotaId}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Sanitizer
import org.junit.Before

class ClientIdQuotaTest extends BaseQuotaTest {

  override val userPrincipal = KafkaPrincipal.ANONYMOUS.getName
  override def producerClientId = "QuotasTestProducer-!@#$%^&*()"
  override def consumerClientId = "QuotasTestConsumer-!@#$%^&*()"
  override val producerQuotaId = QuotaId(None, Some(producerClientId), Some(Sanitizer.sanitize(producerClientId)))
  override val consumerQuotaId = QuotaId(None, Some(consumerClientId), Some(Sanitizer.sanitize(consumerClientId)))

  @Before
  override def setUp() {
    this.serverConfig.setProperty(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp, defaultProducerQuota.toString)
    this.serverConfig.setProperty(KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp, defaultConsumerQuota.toString)
    super.setUp()
  }
  override def overrideQuotas(producerQuota: Long, consumerQuota: Long, requestQuota: Double) {
    val producerProps = new Properties()
    producerProps.put(DynamicConfig.Client.ProducerByteRateOverrideProp, producerQuota.toString)
    producerProps.put(DynamicConfig.Client.RequestPercentageOverrideProp, requestQuota.toString)
    updateQuotaOverride(producerClientId, producerProps)

    val consumerProps = new Properties()
    consumerProps.put(DynamicConfig.Client.ConsumerByteRateOverrideProp, consumerQuota.toString)
    consumerProps.put(DynamicConfig.Client.RequestPercentageOverrideProp, requestQuota.toString)
    updateQuotaOverride(consumerClientId, consumerProps)
  }
  override def removeQuotaOverrides() {
    val emptyProps = new Properties
    updateQuotaOverride(producerClientId, emptyProps)
    updateQuotaOverride(consumerClientId, emptyProps)
  }

  private def updateQuotaOverride(clientId: String, properties: Properties) {
    adminZkClient.changeClientIdConfig(Sanitizer.sanitize(clientId), properties)
  }
}
