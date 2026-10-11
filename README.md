# RabbitMQ: Sıfırdan Modern Mesajlaşma Mimarisi (.NET Odaklı)

> **Öğrenme rehberi • Yerel kurulum • AMQP 0-9-1 • Exchange türleri • Queue yaşam döngüsü • Mesaj güvenilirliği**  
> **Güncelleme:** 11 Ekim 2026  
> **Hedef ortam:** RabbitMQ **4.3.6**, RabbitMQ.Client **7.2.2**, Docker Compose, modern C# / .NET  
> **Bu aşama:** Kavramları öğrenmek, broker'ı kurmak ve Management UI'da deney yapmak. Producer/Consumer uygulamasını sonraki aşamada sıfırdan yazacağız.

RabbitMQ, uygulamaların birbiriyle **asenkron**, **gevşek bağlı (loosely coupled)** ve gerektiğinde **güvenilir** biçimde haberleşmesine yardımcı olan bir **message broker**'dır. Bu rehber, kavramları sadece tanımlamakla kalmaz; **mesaj nereye gider, ne zaman kaybolabilir, hangi ayar neyi değiştirir?** sorularını adım adım cevaplar.

> ⚠️ **Önemli:** RabbitMQ kullanmak tek başına mesaj kaybını önlemez. Dayanıklılık, doğru routing, publisher confirms, consumer acknowledgements, disk/volume yönetimi ve uygulamanın hata stratejisi birlikte tasarlanmalıdır.

## İçindekiler

1. [Hedef, senaryo ve temel model](#1-hedef-senaryo-ve-temel-model)
2. [Windows'ta Docker ile kurulum](#2-windowsta-docker-ile-kurulum)
3. [Docker Compose dosyası: satır satır](#3-docker-compose-dosyası-satır-satır)
4. [Management UI'ı tanımak](#4-management-uiı-tanımak)
5. [AMQP, connection, channel ve vhost](#5-amqp-connection-channel-ve-vhost)
6. [Exchange, queue, binding ve routing key](#6-exchange-queue-binding-ve-routing-key)
7. [Exchange türleri: Direct, Fanout, Topic, Headers](#7-exchange-türleri-direct-fanout-topic-headers)
8. [Default exchange ve diğer exchange özellikleri](#8-default-exchange-ve-diğer-exchange-özellikleri)
9. [Queue türleri, özellikleri ve yaşam döngüsü](#9-queue-türleri-özellikleri-ve-yaşam-döngüsü)
10. [Mesajlar ne zaman silinir, ne zaman kalır?](#10-mesajlar-ne-zaman-silinir-ne-zaman-kalır)
11. [ACK, NACK, reject ve prefetch](#11-ack-nack-reject-ve-prefetch)
12. [Publisher confirms, mandatory, güvenilir yayın](#12-publisher-confirms-mandatory-güvenilir-yayın)
13. [TTL, DLX, retry, poison message](#13-ttl-dlx-retry-poison-message)
14. [Mesaj sırası, çoklu consumer ve ölçekleme](#14-mesaj-sırası-çoklu-consumer-ve-ölçekleme)
15. [C# / RabbitMQ.Client 7.x API önizlemesi](#15-c--rabbitmqclient-7x-api-önizlemesi)
16. [Management UI ile 6 laboratuvar](#16-management-ui-ile-6-laboratuvar)
17. [Operasyon, güvenlik ve sorun giderme](#17-operasyon-güvenlik-ve-sorun-giderme)
18. [Sık sorular, sözlük, öğrenme planı](#18-sık-sorular-sözlük-öğrenme-planı)
19. [Resmî kaynaklar](#19-resmî-kaynaklar)

---

## 1. Hedef, senaryo ve temel model

Bir web uygulaması kullanıcıdan rapor oluşturma isteği alsın. Rapor 30 saniye sürüyorsa HTTP isteğini 30 saniye açık tutmak yerine **iş isteğini** RabbitMQ'ya bırakabiliriz:

```text
┌──────────────────┐      Publish       ┌─────────────────────┐
│ ASP.NET Core API │ ─────────────────> │ RabbitMQ Exchange   │
└──────────────────┘                     └──────────┬──────────┘
        │                                           │ Routing
        │ HTTP 202 Accepted                         ▼
        ▼                                ┌─────────────────────┐
     Kullanıcı                           │ reports.queue       │
                                         └──────────┬──────────┘
                                                    │ Deliver
                                                    ▼
                                         ┌─────────────────────┐
                                         │ .NET Background     │
                                         │ Worker / Consumer   │
                                         └─────────────────────┘
```

**Producer/Publisher** mesajı üreten/yayımlayan uygulamadır. **Exchange** mesajı uygun hedeflere **yönlendirir**. **Queue** mesajı tüketiciye teslim edilene ve işlenene kadar tutabilir. **Consumer** mesajı alıp işlemi yapar. **Binding** exchange ile queue arasındaki routing kuralıdır.

**Kritik ayrım:** RabbitMQ bir **iş tamamlandı veritabanı** değildir. `202 Accepted`, işlemin bittiği değil, asenkron iş için isteğin kabul edildiği anlamına gelebilir. Nihai başarı/durum takibi uygulama tarafından ayrıca tasarlanmalıdır.

### RabbitMQ ne zaman mantıklı?

- Uzun süren işleri background worker'a aktarmak.
- Uygulamaları aynı anda ayakta olmak zorunda bırakmadan haberleştirmek.
- Bir olayı farklı bağımsız servislerin işlemesini sağlamak.
- İş yükünü birden çok worker'a paylaştırmak.
- Retry / dead-letter / acknowledgement üzerinden hataları yönetmek.

**RabbitMQ her sorunun çözümü değildir:** küçük ve senkron işlemler için normal bir API çağrısı yeterli olabilir; geçmiş mesajları yeniden okuma ve uzun süreli event log ihtiyacında Kafka veya RabbitMQ Streams daha uygun olabilir.

---

## 2. Windows'ta Docker ile kurulum

Bu rehberde önerilen yol **Docker Desktop + Docker Compose**. Windows'a RabbitMQ ve Erlang'ı ayrı ayrı yükleme karmaşasını azaltır. Windows üzerinde Linux container kullanmak için Docker Desktop'ın WSL 2 altyapısını hazır bulundurması gerekir.

### 2.1 Ön koşullar

1. **Docker Desktop** kur; çalıştır ve Linux containers modunda olduğundan emin ol.
2. Gerekirse WSL 2 kurulumunu tamamla ve bilgisayarı yeniden başlat.
3. PowerShell açıp kontrol et:

```powershell
docker --version
docker compose version
docker info
```

`docker info` hata veriyorsa Docker engine çalışmıyor olabilir. Bir Linux container kullanabilmelisin.

> Alternatif olarak Windows'a native RabbitMQ + uyumlu Erlang/OTP kurulabilir; ancak öğrenme ve tekrar üretilebilir yerel ortam için Docker tercih ediyoruz. Native kurulumun güncel uyumluluk bilgileri için resmi Windows installation guide'a bak.

### 2.2 Çalışma klasörünü oluştur

```powershell
mkdir RabbitMQ-Learning
cd RabbitMQ-Learning
```

Klasörde iki dosya bulunsun: `docker-compose.yml` ve `.env`.

### 2.3 `docker-compose.yml`

```yaml
services:
  rabbitmq:
    image: rabbitmq:4.3.6-management
    container_name: rabbitmq-local
    hostname: rabbitmq-local
    restart: unless-stopped
    ports:
      - "127.0.0.1:5672:5672"
      - "127.0.0.1:15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: ${RABBITMQ_USER}
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASSWORD}
      RABBITMQ_DEFAULT_VHOST: ${RABBITMQ_VHOST}
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "check_running"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 20s

volumes:
  rabbitmq_data:
```

Bu dosyanın bir kopyası repoya eklemek için ayrıca sunulmuştur.

### 2.4 `.env`

```dotenv
RABBITMQ_USER=devuser
RABBITMQ_PASSWORD=localRabbit123!
RABBITMQ_VHOST=learning
```

Bu bilgiler **yalnızca yerel eğitim içindir**. Gerçek şifreleri GitHub'a koyma. `.env` dosyasını `.gitignore` içine al; repoda örnek olarak `.env.example` tut.

```gitignore
.env
```

### 2.5 Broker'ı başlat

```powershell
docker compose up -d
docker compose ps
docker compose logs -f rabbitmq
```

- `up`: Servisi oluşturur/çalıştırır.
- `-d`: Arka planda çalıştırır.
- `ps`: Container durumunu gösterir.
- `logs -f`: Yeni log satırlarını canlı izler (`Ctrl+C` log takibini bitirir, container'ı kapatmaz).

Ayrıca:

```powershell
docker exec rabbitmq-local rabbitmq-diagnostics -q ping
docker exec rabbitmq-local rabbitmqctl status
```

**Management UI:** `http://localhost:15672`  
**Kullanıcı:** `devuser`  
**Şifre:** `.env` içindeki `RABBITMQ_PASSWORD`  
**Virtual host:** `learning`  
**AMQP bağlantısı:** `localhost:5672`

### 2.6 Testi bitirince ne yapacaksın?

```powershell
docker compose stop          # Container durur; veri korunur
docker compose start         # Yeniden çalıştır
docker compose restart       # Yeniden başlat
docker compose down          # Container/network kaldırılır; named volume kalır
docker compose down -v       # DİKKAT: Named volume da silinir, RabbitMQ verileri kaybolur
```

⚠️ `docker compose down -v` eğitimde bilerek temiz başlangıç yapmak için yararlıdır ama kalıcı mesajların, kuyrukların, kullanıcıların ve diğer broker durumunun silinmesine yol açar.

---

## 3. Docker Compose dosyası: satır satır

| Alan | Açıklama | Değiştirirsen ne olur? |
|---|---|---|
| `services.rabbitmq` | Compose içindeki servis adı. | Aynı ağdaki diğer container'lar bu servis adına bağlanabilir. |
| `image: rabbitmq:4.3.6-management` | RabbitMQ 4.3.6 ve Management eklentisi olan image. | Sabit tag tekrarlanabilir ortam sağlar; `latest` sürpriz güncelleme yapabilir. |
| `container_name` | Docker'da görülen ad. | `docker exec rabbitmq-local ...` komutlarının hedefidir. |
| `hostname` | Container'ın RabbitMQ node kimliğinde kullanılan host adını sabitler. | Kalıcı veriyle yeniden oluşturmalarda node kimliğinin değişmesini önlemeye yardımcı olur. |
| `restart: unless-stopped` | Docker yeniden başladığında container'ı genelde tekrar ayağa kaldırır. | Elle durdurduğunda tekrar açılmaması beklenir. |
| `127.0.0.1:5672:5672` | Host'taki localhost 5672 → container 5672 (AMQP). | C# producer/consumer bağlanır; yalnızca yerel bilgisayardan erişim hedeflenir. |
| `127.0.0.1:15672:15672` | Host'taki localhost 15672 → container 15672 (HTTP Management). | Tarayıcıyla yönetim ekranı açılır. |
| `environment` | İlk başlatmada kullanıcı, parola ve vhost kurulum değerleri. | Var olan volume'da kullanıcıyı/parolayı sırf `.env` değiştirerek güncellemiş olmazsın. |
| `rabbitmq_data:/var/lib/rabbitmq` | Broker verisini Docker named volume'da saklar. | Container silinse bile volume durursa verinin yaşama şansı vardır. |
| `healthcheck` | RabbitMQ servisinin çalışırlığını kontrol eder. | `healthy`, uygulamanın her olası bağlantı davranışının kusursuz olduğunu garanti etmez. |
| `interval/timeout/retries/start_period` | Healthcheck sıklığı, süre sınırı, tekrar sayısı, başlangıç toleransı. | Yanlış kısa değerler başlangıçta yanlış `unhealthy` üretebilir. |

### Neden iki port?

- **5672:** AMQP 0-9-1 (C# uygulamamızın bağlanacağı port).
- **15672:** Management UI ve HTTP API.
- **5671:** TLS üzerinden AMQP için yaygın port; mevcut örnekte açmadık.
- **25672 / 4369:** Cluster/distribution ve Erlang discovery için kullanılan portlar; tek node yerel eğitimde host'a açmaya gerek yok.
- **5552:** RabbitMQ Stream protokolü eklentisiyle ilgili port; bu rehberin temel AMQP adımında gerekli değil.

**Önemli ağ ayrımı:** Bilgisayarında çalışan .NET uygulaması `localhost:5672` kullanır. Aynı Compose ağına eklenen başka bir container ise `rabbitmq:5672` kullanır. Container içinden `localhost`, RabbitMQ container'ı değil **o container'ın kendisi** demektir.

---

## 4. Management UI'ı tanımak

`http://localhost:15672` adresinde giriş yaptıktan sonra sürüme göre menü adları değişebilse de şu bölümleri görürsün:

| Menü | Ne görürsün? | Ne için kullanılır? |
|---|---|---|
| **Overview** | Bağlantılar, publish/deliver oranları, kuyruk/message toplamları, node kaynakları. | Sistemin genel sağlık resmi. |
| **Connections** | TCP/AMQP connection'lar. | Uygulamaların broker'a bağlı olup olmadığı. |
| **Channels** | Her connection içindeki AMQP channel'lar. | Channel sayısı ve activity gözlemi. |
| **Exchanges** | Exchange listesi, türü, binding'ler, publish alanı. | Routing deneyi yapma. |
| **Queues and Streams** | Queue/stream listesi, Ready/Unacked, consumers, bindings. | Mesajların hangi aşamada olduğuna bakma. |
| **Admin** | Kullanıcılar, vhost'lar, izinler, policies. | Güvenlik, yönetim ve kurallar. |

### Ready / Unacked / Total ne demek?

| Durum | Anlamı |
|---|---|
| **Ready** | Queue'da bulunup consumer'a teslim edilmeyi bekleyen mesaj. |
| **Unacked** | Consumer'a teslim edilmiş ama broker henüz başarılı ACK almamış mesaj. |
| **Total** | Genellikle Ready + Unacked. |

**Örnek:** `Ready=7`, `Unacked=3` → toplamda bu queue ile ilişkili 10 bekleyen/in-flight mesaj görünüyor. `Unacked=3` mesajlar consumer'ın kalıcı olarak sahiplendiği anlamına **gelmez**; consumer düşerse tekrar teslim edilebilir.

### İlk önemli gözlem

Broker açıldığında henüz hiç application connection olmayabilir. Default exchange ve `amq.*` gibi önceden oluşturulmuş sistem exchange'leri görebilirsin. Bunlar uygulamanın senin adına yarattığı her yeni exchange değildir.

---

## 5. AMQP, connection, channel ve vhost

### AMQP 0-9-1 nedir?

RabbitMQ'nun yaygın mesajlaşma protokolüdür. Producer, broker ve consumer arasındaki komutları tanımlar: exchange/queue deklarasyonu, publish, consume, ACK, binding vb. RabbitMQ başka protokolleri de destekler; bu eğitimde öncelikle **AMQP 0-9-1** kullanıyoruz.

### Connection ile Channel farkı

```text
.NET Application
   └── IConnection  (TCP bağlantısı)
       ├── IChannel #1 (publish/topology)
       └── IChannel #2 (consume)
                  │
                  ▼
            RabbitMQ Broker
```

- **Connection:** Broker'a kurulan fiziksel/ağ seviyesindeki bağlantı; tekrar tekrar açmak maliyetlidir.
- **Channel:** Connection üzerinden çoğullanan mantıksal AMQP oturumu; publish, queue/exchange declare ve consume gibi işler bununla yapılır.
- Genellikle **uzun ömürlü connection** ve görevlerin ihtiyaçlarına göre yönetilen channel'lar tercih edilir.
- **Bir `IChannel` örneğini çoklu publisher thread'lerine eşzamanlı ve korunmasız biçimde verme.** 7.x istemcide frame interleaving ve protokol hatalarına yol açabilir.
- Bir uygulama hem producer hem consumer olabilir.

### Virtual host (vhost)

Vhost'u bir **mantıksal izolasyon alanı** gibi düşün. `learning` içindeki `orders.queue` ile başka vhost içindeki aynı adlı queue ayrı kaynaklardır. Exchange, queue, binding ve izinler vhost kapsamındadır.

`.env` ile `learning` vhost'unu oluşturuyoruz. Management UI'da doğru vhost seçili değilse oluşturduğun queue/exchange'i göremeyebilirsin.

### User ve permission

RabbitMQ kullanıcılarının vhost başına **configure**, **write**, **read** gibi izinleri bulunur. Development ortamında geniş izin kolaylık sağlar; production'da sadece gereken izinleri ver.

---

## 6. Exchange, queue, binding ve routing key

```text
                                  binding key = order.created
                                 ┌───────────────────────────┐
Publisher ── routing key ──> Exchange                          ▼
           order.created          │                       orders.queue
                                  │
                                  └────── (başka binding) ─> audit.queue
```

- **Exchange:** Gelen mesajı bir veya daha fazla hedefe route eder. Geleneksel AMQP modeli için kalıcı mesaj deposu **değildir**.
- **Queue:** Consume edilmek için bekleyen mesajları tutar.
- **Binding:** Exchange ile destination arasındaki yönlendirme bağlantısı/kuralıdır.
- **Routing key:** Publisher'ın mesajla beraber belirttiği anahtar (`order.created` gibi).
- **Binding key / pattern:** Bir queue'nun hangi routing key'leri kabul edeceğini belirten bağlama kuralı.

**Örnek:** Publisher `orders.exchange`'e `order.created` anahtarıyla mesaj gönderir. Exchange `order.created` binding'ine sahip kuyruklara yönlendirir. **Tek mesaj, iki farklı queue'ya ayrı kopya** olarak yerleşebilir; fakat **aynı queue'ya birden fazla binding eşleşse bile tek publish için o queue'ya tek kopya** yönlendirilir.

### Exchange'e mesaj gönderdim, hiçbir queue eşleşmedi. Ne olur?

Routing bulunmazsa ve **alternate exchange** tanımlanmamışsa, `mandatory=false` (varsayılan) yayında mesaj **discard** edilebilir. `mandatory=true` ile unroutable mesaj publisher'a **basic.return** olarak döndürülebilir; publisher bunu gerçekten dinlemelidir. Broker'ın mesajı kabul etmesi **uygun bir queue'ya route edildiği** anlamına otomatik gelmez.

---

## 7. Exchange türleri: Direct, Fanout, Topic, Headers

RabbitMQ'da öğrenilmesi gereken dört ana AMQP exchange tipi vardır. Exchange tipi **mesajın kaç queue'ya hangi kuralla gideceğini** belirler; **mesaj kalıcılığını tek başına belirlemez**.

### 7.1 Direct Exchange — tam eşleşme

**Kural:** Mesajın `routingKey` değeri ile queue binding key'i **birebir aynı** olmalı.

```text
                           routingKey = error
Producer ──> logs.direct ──────────────────────> error.queue [key=error]
                      └────────────────────────> audit.queue [key=error]
                      └──X──────────────────────> info.queue  [key=info]
```

| Mesaj routing key'i | Queue binding'i | Sonuç |
|---|---|---|
| `error` | `error` | Eşleşir |
| `error` | `info` | Eşleşmez |
| `order.created` | `order.*` | **Eşleşmez** (`*` direct'te wildcard değil) |

**Kullanım:** belirli iş türlerini belirli work queue'lara yönlendirme, seviyeye göre log yönlendirme. Birden çok queue aynı binding key ile bağlanırsa **her queue bir kopya** alır.

### 7.2 Fanout Exchange — broadcast

**Kural:** Mesajı exchange'e bağlı **tüm** queue'lara gönderir; routing key'i **yok sayar**.

```text
                      ┌──> email.events.queue
Producer ──> events.fanout ──> analytics.events.queue
                      └──> audit.events.queue
```

- Publisher'ın verdiği routing key routing üzerinde etkili değildir.
- **Bir fanout exchange + üç queue = her uygun publish'te üç kopya** (her queue'ya birer kopya).
- **Üç consumer'ın aynı queue'ya bağlanması, üç kopya yaratmaz.** O farklı bir kullanım şeklidir (competing consumers).
- **Consumer offline ama durable queue mevcutsa:** mesaj queue'da bekleyebilir. **Queue hiç yoksa veya artık bağlı değilse:** gelecekte açılan queue geçmiş publish'leri otomatik alamaz.

**Kullanım:** aynı olayın bildirim, analiz ve audit gibi bağımsız servisler tarafından işlenmesi.

### 7.3 Topic Exchange — pattern eşleşmesi

**Kural:** Routing key nokta (`.`) ile ayrılmış sözcükler içerir. Binding pattern'inde:

- `*` → **tam bir** sözcük/segment.
- `#` → **sıfır veya daha fazla** segment.

```text
routing key:  order.created.eu
binding A:    order.*.eu   ✓
binding B:    order.#      ✓
binding C:    payment.#    ✗
binding D:    order.*      ✗ (yalnızca 2 segment ister)
```

| Binding pattern | Routing key | Eşleşir mi? | Gerekçe |
|---|---|---|---|
| `order.*` | `order.created` | ✅ | Tek segment wildcard |
| `order.*` | `order.created.eu` | ❌ | Ek segment var |
| `order.#` | `order` | ✅ | `#` sıfır segment de olabilir |
| `order.#` | `order.created.eu` | ✅ | Çoklu segment |
| `*.created` | `order.created` | ✅ | Başta bir segment |
| `#.failed` | `payment.charge.failed` | ✅ | Sonda `failed` |
| `#` | `anything.anywhere` | ✅ | Hepsini kapsar |

**Kullanım:** olay türü, ortam, bölge ve servis ailesine göre esnek routing (`user.created.eu`, `order.cancelled.tr` gibi).

### 7.4 Headers Exchange — header/özellik karşılaştırması

**Kural:** Routing key yerine **message headers** içindeki anahtar-değer çiftleri incelenir. Binding'in arguments alanındaki `x-match` özelliği önemli:

- `x-match = all`: Belirtilen koşulların **tamamı** eşleşmeli.
- `x-match = any`: Koşullardan **en az biri** eşleşmeli.

**Örnek binding arguments (headers exchange için):**

```json
{
  "x-match": "all",
  "format": "pdf",
  "region": "eu"
}
```

`format=pdf` **ve** `region=eu` header'ları bulunan mesaj eşleşir; sadece biri varsa eşleşmez. Aynı kurallar `x-match=any` iken en az bir koşulu karşılamayı yeterli kılar. Gerçek header değerlerinin AMQP tiplerine ve değer karşılaştırmasına dikkat et.

**Kullanım:** Routing'in doğal olarak routing key hiyerarşisinden değil **birden fazla mesaj metadata alanından** türediği senaryolar. Daha çok mesaj metadatası ve binding karmaşıklığına neden olabildiği için basit hiyerarşik filtrelerde genellikle topic tercih edilir.

### 7.5 Karşılaştırma tablosu

| Tür | Neyle eşleşir? | Bir mesaj kaç queue'ya gidebilir? | Tipik kullanım |
|---|---|---|---|
| **Direct** | Routing key = binding key | 0, 1 veya çok | İş tipine göre kesin routing |
| **Fanout** | Eşleşme gerekmez | Tüm bağlı queue'lar | Olayı herkese duyurma |
| **Topic** | `*` / `#` pattern | 0, 1 veya çok | Esnek event routing |
| **Headers** | Header anahtar-değerleri | 0, 1 veya çok | Metadata tabanlı routing |

**Akılda kalacak nokta:** *Exchange tipi routing stratejisidir; Direct = tek queue, Fanout = tek mesaj, Topic = sadece bir tüketici* gibi çıkarımlar **yanlıştır**.

---

## 8. Default exchange ve diğer exchange özellikleri

### 8.1 Default exchange (`""`)

RabbitMQ bir vhost oluşturulduğunda otomatik olarak **adı boş string (`""`)** olan özel bir **direct exchange** sağlar.

```text
Publish(exchange="", routingKey="reports.queue")
                       │
                       ▼
                  [default exchange]
                       │ (queue adıyla otomatik binding)
                       ▼
                   reports.queue
```

Bir queue declare edildiğinde default exchange'e **queue adı** routing key olacak şekilde otomatik binding yapılır. Bu yüzden bazen “RabbitMQ'da doğrudan queue'ya mesaj gönderiyorum” denir; teknik olarak yine exchange vardır. **Default exchange'i normal uygulama exchange'i gibi yeniden declare etme / özel binding eklemeye çalışma.**

### 8.2 Exchange declare parametreleri

| Parametre | Anlamı | Önerilen başlangıç |
|---|---|---|
| `exchange` | Exchange adı. | `orders.exchange` gibi anlamlı ad |
| `type` | `direct`, `fanout`, `topic`, `headers`. | Senaryoya göre |
| `durable` | Broker restart sonrasında exchange tanımının korunması. | Uzun ömürlü exchange'de `true` |
| `autoDelete` | En az bir binding'i olmuş exchange'in son binding'i kaldırıldığında silinmesi. | Kalıcı iş topolojisinde `false` |
| `arguments` | Opsiyonel x-arguments (alternate exchange gibi). | Başta `null` |
| `internal` | Dahili exchange: doğrudan publisher publish yapamaz (gelişmiş kullanım). | Genelde `false` |
| `passive` (ayrı kontrol çağrısı) | Yeni exchange oluşturmadan mevcut olup olmadığını kontrol etme. | Doğrulama için |

**Dikkat:** `durable=true` exchange'i saklar; **üzerinden geçen mesajı disk üzerinde sonsuza kadar tutmaz.** Mesajların kaderi queue/mesaj ayarlarıyla ilgilidir.

### 8.3 Alternate exchange (AE) vs. Dead Letter Exchange (DLX)

- **Alternate Exchange:** Mesaj **başlangıçtaki exchange'de route edilemezse** devreye girebilen alternatif routing hedefidir.
- **Dead Letter Exchange:** Mesaj **önceden bir queue'ya girdikten sonra** reddedilirse, süresi dolarsa veya belli limitlere takılırsa hedef olabilir.

```text
No matching binding: Producer -> Main Exchange -> Alternate Exchange -> fallback.queue

Consumed then rejected: Producer -> Main Exchange -> main.queue -> DLX -> dead.queue
```

İki mekanizma farklı aşamalarda çalışır. **AE ve DLX aynı şey değildir.** Her iki mekanizma da ayrıca doğru binding/queue konfigürasyonu gerektirir.

---

## 9. Queue türleri, özellikleri ve yaşam döngüsü

### 9.1 Queue declare parametreleri

| Parametre | Etkisi | Ne zaman kullanılır? |
|---|---|---|
| `queue` | Queue adı. Boş ad verilirse broker bazı kullanım biçimlerinde otomatik isim üretebilir. | İsimli durable queue veya geçici reply queue |
| `durable` | Queue **tanımı** broker restart sonrasında korunur. | Kalıcı iş queue'sunda `true` |
| `exclusive` | Sadece onu declare eden **connection** tarafından kullanılır; o connection gidince queue silinir. | Geçici, connection'a özel queue |
| `autoDelete` | Queue **en az bir consumer görmüşse**, son consumer ayrılınca otomatik silinir. | Geçici abonelik queue'su |
| `arguments` | `x-queue-type`, `x-message-ttl`, `x-max-length`, DLX vb. | İleri seviye davranış |

**Dikkat:** `autoDelete=true` demek consumer hiç başlamadan hemen silinecek demek değildir. `exclusive=true` de consumer değil **connection yaşam döngüsü** ile ilgilidir.

### 9.2 Classic queue

- Genel amaçlı queue türüdür; tek node üzerinde başlamak için uygundur.
- Modern RabbitMQ'da classic queue'ların eski **mirrored classic** yüksek erişilebilirlik modeli artık önerilen/var olan çözüm değildir.
- Tek node classic queue **node arızasına karşı HA/replication sağlamaz**.
- Durable classic queue + persistent mesaj, restart senaryosundaki güvenilirliği artırır; **tek disk/nodun kalıcı kaybına karşı replika sağlamaz**.

### 9.3 Quorum queue

- **Raft** tabanlı replicated, durable queue yaklaşımıdır.
- Verinin önemli olduğu ve birden fazla RabbitMQ node'lu cluster'da yüksek erişilebilirlik istendiği senaryolarda güçlü tercihtir.
- `x-queue-type = quorum` ile seçilebilir; queue tipi bir defa oluşturulduktan sonra basitçe değiştirilemez.
- **Her zaman durable** olmalıdır; exclusive geçici queue mantığıyla kullanılmaz.
- Bir **tek node** üzerinde quorum queue kurmak, yalnızca adını `quorum` yaptığın için HA sağlamaz. Replikasyon için birden fazla node ve sağlıklı çoğunluk gerekir.
- Farklı kaynak kullanımı, performans ve özellik sınırlamaları vardır; “her durumda classic'ten daha iyi” gibi değerlendirilmemeli.

### 9.4 Stream — neden ayrı?

Normal classic/quorum queue'da **mesaj başarıyla tüketilip ACK edildikten sonra queue'dan kaldırılır**. RabbitMQ **Streams** ise append-only log yaklaşımıyla mesajların retention süresince **tekrar okunmasını** sağlar. Stream'ler *normal work queue'nun sadece başka isimlisi* değildir.

```text
Classic / Quorum queue: Publish -> Consume -> ACK -> Queue'dan çıkar
Stream:                 Publish -> Log'a ekle -> Consumer offset'ten okur
                                                Başka consumer tekrar okuyabilir
```

Uzun süreli saklama, replay, büyük backlog veya aynı olayın çok sayıda bağımsız consumer tarafından yeniden okunması gerekiyorsa stream'leri ayrıca öğrenmek gerekir.

### 9.5 RabbitMQ 4.3 için güncel ve kritik not

**RabbitMQ 4.3.0 itibarıyla `durable=false` ve `exclusive=false` olan (non-exclusive transient) classic queue'lar deprecated olup varsayılan ayarlarla declare edilemez.** Eski eğitimlerin `QueueDeclare(queue, durable:false, exclusive:false, autoDelete:false)` kodları yeni broker'da başarısız olabilir.

Öğrenme örneklerimizde normal work queue'lar için **`durable:true, exclusive:false, autoDelete:false`** kullanacağız. Gerçekten geçici queue gerektiğinde server-named **exclusive** queue veya TTL'li durable queue tercih edebiliriz. Eski davranışı geri açan geçici uyumluluk ayarı bulunsa da yeni projede bunu temel almak doğru değildir.

---

## 10. Mesajlar ne zaman silinir, ne zaman kalır?

Bu bölüm, RabbitMQ'yu öğrenirken en çok karıştırılan konunun cevabıdır.

### 10.1 Üç farklı “kalıcılık” kavramı

**1. Exchange durability:** Routing nesnesi restart sonrasında yeniden vardır.  
**2. Queue durability:** Queue tanımı restart sonrasında yeniden vardır.  
**3. Message persistence:** Mesaj, uygun kalıcı saklama davranışını kullanacak şekilde publish edilmiştir (AMQP 0-9-1 classic için `deliveryMode=2`).

Bunlar **birbirinin yerine geçmez**.

### 10.2 Senaryo tablosu

| Senaryo | Sonuç |
|---|---|
| **Consumer yok**, durable queue var, mesaj doğru route edilmiş | Mesaj, TTL/limit/durum değişmedikçe queue'da bekleyebilir. |
| Mesaj consumer'a gitti, **manual ACK henüz yok** | `Unacked` görünür. Consumer/connection düşerse yeniden teslim edilebilir. |
| Consumer başarıyla işleyip **ACK** gönderdi, broker ACK'i aldı | Mesaj normal queue'dan kaldırılır. |
| `autoAck=true`, teslim gerçekleşti, consumer işlem sırasında çöktü | Mesaj kaybedilebilir; broker işleme sonucunu beklemez. |
| Durable queue + **transient message**, broker restart oldu | Transient mesajlar recovery'de atılır. |
| **Non-durable queue**, broker restart oldu | Queue ve içindeki mesajlar kaybolur. (4.3'te yukarıdaki kısıtı unutma.) |
| Durable queue + **persistent message**, broker normal şekilde restart oldu, volume duruyor | Uygun şekilde kabul edilip saklanan mesajların toparlanması beklenir; publisher confirms kullan. |
| Exchange var ama **eşleşen queue/binding yok** | AE/mandatory mekanizması yoksa mesaj discard edilebilir. |
| Queue **TTL** ile silindi | Queue ve içindeki mesajlar silinir. Sırf queue expire oldu diye içindeki mesajlar DLX'e taşınmaz. |
| Mesajın **TTL** süresi doldu | Mesaj süresi dolunca queue'dan atılır veya DLX'e yönlendirilir (varsa). |
| `docker compose down` | Named volume korunur; tekrar ayağa kaldırınca veri genelde geri gelir. |
| `docker compose down -v` | Named volume silinir; disk üzerindeki RabbitMQ verisi kaybolur. |
| Queue **purge** edildi | Ready mesajları temizlenir; unacked olanların davranışı ayrı değerlendirilmelidir. |
| Queue **delete** edildi | Queue ve içindeki mesajlar kaldırılır. |

### 10.3 Altın kural

**Durable exchange + durable queue + persistent message + kalıcı volume + publisher confirms + manual consumer ACK** birlikte düşünülmelidir. Bu kombinasyon bile **uçtan uca exactly-once** garantisi vermez; ağ kopması ve ACK belirsizliği nedeniyle duplicate mesaj mümkündür.

> **İşlem bir kere yapılmışken aynı mesaj tekrar gelirse ne olacak?** Bunun cevabı consumer tarafında **idempotency** olmalıdır; ör. `MessageId` için benzersiz işleme kaydı.

### 10.4 “Mesaj tüketilince silinir mi?” sorusuna kesin cevap

**Klasik iş queue'larında**, evet: mesaj **başarılı ACK broker'a ulaştığında** normal akışta queue'dan kaldırılır. Durable olmak **tüketilmiş mesajları geçmişe dönük arşivlemek** demek değildir. Replay ihtiyacını normal queue ile değil, stream/event log yaklaşımıyla ele al.

---

## 11. ACK, NACK, reject ve prefetch

### 11.1 Consumer acknowledgement

- **Manual ACK:** İş gerçekten başarıyla tamamlandıktan sonra consumer mesajı onaylar.
- **Auto ACK:** Broker, teslimden sonra uygulama başarısını beklemeden mesajı onaylanmış sayar; kritik işlerde kayıp riski doğurabilir.
- **NACK / Reject:** Başarısızlığı bildirir; `requeue` ayarı mesajın tekrar queue'ya girip girmeyeceğini belirler.

```text
Queue --deliver--> Consumer
                    ├── success --------------------> ACK --------> kaldır
                    ├── temporary failure ---------> NACK requeue=true -> yeniden sıraya
                    └── permanent failure ---------> NACK requeue=false -> DLX / discard
```

**`BasicAckAsync(deliveryTag, multiple)`**: `deliveryTag` belirli channel üzerindeki teslim numarasıdır; `multiple=true` aynı channel üzerindeki ilgili etikete kadar onaylama kapsamı yaratabilir. Yanlış toplu ACK, henüz işlenmemiş mesajları yanlışlıkla onaylayabilir.

**`BasicNackAsync(deliveryTag, multiple, requeue)`**: Red/başarısızlık bildirir. `requeue=true` mesajı yeniden tüketim için sunar, `false` dead-letter veya discard davranışına gider.

**`BasicRejectAsync(deliveryTag, requeue)`**: Bir mesajı reddetme yolu; `multiple` parametresi yoktur.

### 11.2 Prefetch nedir?

Consumer'ın aynı anda **kaç adet ACK bekleyen mesaj** teslim alabileceğine bir sınır koyar. Örnek `prefetchCount=10` → o consumer'a henüz ACK edilmemiş en fazla yaklaşık 10 mesaj teslim edilir; RabbitMQ'nun per-consumer yorumlama kurallarına tabidir.

- **Çok düşük değer:** Kapasite yetersiz kullanılabilir.
- **Çok yüksek değer:** Consumer bellek yükü, adaletsiz dağıtım veya çok sayıda unacked mesaj oluşabilir.
- **`0`:** Sınır olmaması anlamına gelir; başlangıç için çoğunlukla uygun değildir.
- **`global=false`:** Yaygın RabbitMQ kullanımında per-consumer limit yaklaşımı. Quorum queue global QoS özelliğini desteklemez.

**Örnek:** 30 Ready mesaj var, iki consumer var, her birinin prefetch'i 5. İlk aşamada toplam en fazla 10 mesaj Unacked olabilir. Bu **her consumer sürekli eşit sayıda iş alacak** garantisi değildir; işleme hızı ve diğer koşullar dağılımı etkiler.

---

## 12. Publisher confirms, mandatory, güvenilir yayın

### 12.1 Bir `PublishAsync` çağrısı yaptım; mesaj kesin güvende mi?

Tek başına çağrı yapmak veya ağa byte yazılması, RabbitMQ'nun mesajı güvenli biçimde kabul ettiğinin **garantisi değildir**. **Publisher confirms**, broker'ın mesaj için sorumluluk aldığını producer'a bildirmesini sağlar. Özellikle veri kaybını önemseyen uygulamalarda kullanılır.

### 12.2 `mandatory` başka bir şeyi çözer

- `mandatory=false`: Eşleşen hedef yoksa mesaj discard edilebilir (veya alternate exchange'e gidebilir).
- `mandatory=true`: Route edilemeyen mesaj için broker `basic.return` gönderebilir. Publisher returned-message olayını dinlemeli ve sonucu ele almalıdır.
- **Publisher confirm = routing success garantisi değildir.** `mandatory` / AE ayrı konudur.

### 12.3 Teslimat garantileri

| Terim | Anlamı | Risk |
|---|---|---|
| **At-most-once** | En fazla bir teslim; tekrar denememe yaklaşımı. | Mesaj kaybı olabilir. |
| **At-least-once** | Başarıya ulaşana kadar yeniden teslim mümkün. | Aynı mesaj iki kez işlenebilir. |
| **Exactly-once** | İş etkisinin tam bir kez gerçekleşmesi hedefi. | RabbitMQ'nun tek bir ayarıyla uçtan uca garanti edilmez. |

**Kritik failure window:** Consumer DB'ye yazdı → uygulama ACK göndermeden düştü → broker mesajı yeniden teslim etti. Consumer işi **idempotent** tasarlamalı; gerektiğinde Inbox/Outbox ve transaction tasarımı düşünülmelidir.

### 12.4 Bir mesaj birden çok queue'ya route edildiyse?

Her queue mesaj kopyasını **bağımsız** yaşatır. Bir queue'da ACK edilen teslim, başka queue'daki kopyayı otomatik silmez.

---

## 13. TTL, DLX, retry, poison message

### 13.1 TTL (Time To Live)

İki farklı TTL'yi ayır:

1. **Message TTL (`x-message-ttl`)**: Queue içindeki mesajların bekleme süresini sınırlayabilir. Değer **milisaniye**; `60000` = 60 saniye.
2. **Queue TTL (`x-expires`)**: Kullanılmayan queue'nun belirli koşullarda otomatik silinmesini sağlar; mesaj TTL'si değildir.

Mesaj bazında `BasicProperties.Expiration = "60000"` olarak da TTL verilebilir. Hem queue hem mesaj seviyesinde varsa **daha kısa** geçerli olur. TTL dolduğunda mesaj her koşulda o milisaniyede anında fiziksel olarak silinmiş görünmeyebilir; RabbitMQ'nun expiry zamanlamasını bilmek önemlidir.

### 13.2 DLX ve DLQ

- **Dead Letter Exchange (DLX)**: Başarısız/süresi dolmuş mesajın yeniden route edildiği **normal exchange**.
- **Dead Letter Queue (DLQ)**: DLX'in mesajı yönlendirdiği queue için kullanılan yaygın isim. DLQ teknik olarak yine normal queue'dur.

DLX'e yönlendirilebilen durumlara örnekler:

- `BasicReject` / `BasicNack` ile `requeue=false`.
- Message TTL'nin dolması.
- Queue length limit nedeniyle mesajın düşürülmesi (konfigürasyona göre).
- Quorum queue delivery-limit (poison message) davranışları.

```text
Publisher -> orders.exchange -> orders.queue
                                  │
                              NACK(false)
                                  │
                                  ▼
                            orders.dlx
                                  │
                                  ▼
                           orders.dead.queue
```

**DLX otomatik retry değildir.** Tekrar denenecek mesajları yeniden işleyen akışı ayrıca kurmalısın. Örneğin “ana queue -> retry queue (TTL) -> ana queue” olabilir; ancak döngü yönetimi, deneme sayısı ve gözlemlenebilirlik eklenmezse sonsuz retry oluşabilir.

### 13.3 `requeue=true` neden tehlikeli olabilir?

Mesaj her denemede aynı nedenle hata veriyorsa hemen requeue etmek yoğun **redelivery loop** yaratır; CPU/network harcanır, diğer mesajlar aç kalabilir. Sınırlı retry, gecikme/backoff ve sonra DLQ yaygın çözüm stratejileridir.

### 13.4 Policy mi, `x-arguments` mı?

RabbitMQ, dinamik olarak yönetilmesi uygun olan TTL, DLX gibi seçeneklerde **policy** kullanımını sıklıkla önerir. Queue declaration sırasında sabit `x-arguments` yazmak daha sonradan değişiklikte yeniden oluşturma ihtiyacı doğurabilir. Ancak `x-queue-type` gibi bazı alanlar queue yaratılırken belirlenmelidir.

### 13.5 Dead-lettering de kayıp yaşayabilir

DLX'e geçiş de mesaj yayımlama işlemidir. Hedefte sorun varsa veri kaybı veya gecikme riski doğabilir. Özellikle kritik cluster senaryosunda **quorum queue at-least-once dead-lettering** özelliğini ayrıca değerlendirmek gerekir; sırf DLX var diye sınırsız garanti varsayma.

---

## 14. Mesaj sırası, çoklu consumer ve ölçekleme

### 14.1 Aynı queue'ya iki consumer bağlanırsa

```text
                   ┌─> Worker A
orders.queue ──────┤
                   └─> Worker B
```

RabbitMQ mesajları consumer'lara dağıtır. **Bu bir broadcast değildir.** Bir mesaj teslimi genellikle consumer'lardan birine gider; hata/tekrar teslim nedeniyle aynı mesaj başka consumer'a yeniden gelebilir.

### 14.2 Fanout + iki queue farklıdır

```text
                       ┌─> email.queue ──> Email Worker
orders.fanout ──────────┤
                       └─> audit.queue ──> Audit Worker
```

Burada olayın **iki ayrı queue kopyası** vardır. Bir queue'nun işlemesi diğer queue'nun mesajını kaldırmaz.

### 14.3 FIFO garanti mi?

Queue FIFO davranışını baz alabilir ancak **birden fazla consumer, prefetch, yeniden teslim, priority ve paralel işleme** varsa nihai **işin tamamlanma sırası** publish sırasıyla aynı olmayabilir. Kesin seri işleme istiyorsan ilgili iş akışında **Single Active Consumer**, uygun queue tasarımı ve iş mantığı seviyesinde sıra/idempotency politikaları değerlendirilmelidir.

### 14.4 Backpressure

Producer'ın hızı consumer'lardan yüksekse Ready mesaj sayısı büyür. Prefetch yalnızca **consumer'a teslim edilmiş fakat ACK edilmemiş** mesaj sayısını sınırlar; producer'ın sınırsız mesaj üretmesini tek başına durdurmaz. Queue limitleri, publisher flow control, kaynak alarmları ve izleme gerekir.

---

## 15. C# / RabbitMQ.Client 7.x API önizlemesi

> **Bu bölüm tam Producer/Consumer projesi değildir.** Eski eğitimlerde gördüğün methodların güncel karşılığını ve parametrelerin anlamını öğrenmek için küçük, açıklamalı API parçalarıdır. Uçtan uca kodlamayı sonraki aşamada yapacağız.

### 15.1 Paket ve 6.x → 7.x temel farkı

```powershell
dotnet add package RabbitMQ.Client --version 7.2.2
```

| Eski eğitimlerde | Modern 7.x yaklaşımı |
|---|---|
| `factory.CreateConnection()` | `await factory.CreateConnectionAsync()` |
| `connection.CreateModel()` ve `IModel` | `await connection.CreateChannelAsync()` ve `IChannel` |
| `model.ExchangeDeclare(...)` | `await channel.ExchangeDeclareAsync(...)` |
| `model.QueueDeclare(...)` | `await channel.QueueDeclareAsync(...)` |
| `model.QueueBind(...)` | `await channel.QueueBindAsync(...)` |
| `EventingBasicConsumer` / sync events | `AsyncEventingBasicConsumer` + `ReceivedAsync` |
| `model.BasicAck(...)` | `await channel.BasicAckAsync(...)` |

### 15.2 Connection oluşturma

```csharp
using RabbitMQ.Client;

var factory = new ConnectionFactory
{
    HostName = "localhost",   // C# uygulaması host makinede çalışıyor
    Port = 5672,              // AMQP portu
    UserName = "devuser",     // .env ile oluşturduğumuz kullanıcı
    Password = "localRabbit123!", // Yalnızca lokal demo! Prod'da secret store kullan
    VirtualHost = "learning" // Compose'ta oluşturduğumuz vhost
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();
```

| Alan | Anlamı |
|---|---|
| `HostName` | Broker'a erişilecek DNS/host. Docker network içindeysen `rabbitmq`. |
| `Port` | AMQP bağlantı portu; Management UI portu **değildir**. |
| `UserName` / `Password` | Broker hesabı. |
| `VirtualHost` | Hangi mantıksal broker alanında işlem yapılacak. |
| `CreateConnectionAsync` | Fiziksel AMQP connection'ı açar. |
| `CreateChannelAsync` | Connection içinde mantıksal channel oluşturur. |

### 15.3 Exchange, queue ve binding tanımlama

```csharp
await channel.ExchangeDeclareAsync(
    exchange: "orders.exchange",
    type: ExchangeType.Direct,
    durable: true,
    autoDelete: false,
    arguments: null);

await channel.QueueDeclareAsync(
    queue: "orders.queue",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

await channel.QueueBindAsync(
    queue: "orders.queue",
    exchange: "orders.exchange",
    routingKey: "order.created",
    arguments: null);
```

**Neden bu sıra?** Önce exchange ve queue oluşturulur, sonra aralarında routing bağı kurulur. `QueueDeclareAsync` mesaj göndermek değil, queue topolojisi tanımlamaktır.

**`QueueBindAsync` parametreleri:** `queue` hedef, `exchange` kaynak, `routingKey` direct'te tam eşleşen bağlama anahtarı/topic'te pattern, `arguments` ise (özellikle headers binding'inde) ek eşleştirme bilgisidir.

### 15.4 Mesaj properties ve publish

```csharp
using System.Text;

var payload = Encoding.UTF8.GetBytes("Order 42 created");

var properties = new BasicProperties
{
    ContentType = "text/plain",
    DeliveryMode = 2, // AMQP 0-9-1 persistent message
    MessageId = Guid.NewGuid().ToString()
};

await channel.BasicPublishAsync(
    exchange: "orders.exchange",
    routingKey: "order.created",
    mandatory: true,
    basicProperties: properties,
    body: payload);
```

| Parametre | Anlamı |
|---|---|
| `exchange` | Mesajın gönderildiği exchange. |
| `routingKey` | Exchange'in routing sırasında kullandığı anahtar. |
| `mandatory` | Eşleşen hedef bulunamazsa mesajın publisher'a dönmesini isteme. Returned mesajı işlemeyi ayrıca tasarla. |
| `basicProperties` | `DeliveryMode`, `ContentType`, `MessageId`, `CorrelationId`, `Headers`, `Expiration` gibi metadata. |
| `body` | Asıl mesaj içeriği (`byte[]` / `ReadOnlyMemory<byte>`). |

**`DeliveryMode = 2` ne demek?** Persistent publish isteği. Tek başına durable queue, durable volume veya publisher confirms yerine **geçmez**. `MessageId` ise otomatik deduplication yaptırmaz; uygulama bu ID'yi idempotency için kullanabilir.

### 15.5 Consumer API'sine kısa bakış

```csharp
using RabbitMQ.Client.Events;

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (_, ea) =>
{
    // 7.x'te ea.Body: ReadOnlyMemory<byte>
    var body = ea.Body.ToArray(); // Callback sonrası da kullanacaksan kopyala
    var text = Encoding.UTF8.GetString(body);

    Console.WriteLine(text);

    // Gerçek uygulamada: başarılı iş işleminden sonra ACK
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

string consumerTag = await channel.BasicConsumeAsync(
    queue: "orders.queue",
    autoAck: false,
    consumer: consumer);
```

Bu **yalnızca API davranışını göstermek** içindir: consumer'ı ayakta tutan bir host/BackgroundService, graceful shutdown, exception politikası, prefetch ve publisher confirms henüz tasarlanmadı.

- `ReceivedAsync`: Mesaj teslimi callback'i.
- `ea.Body`: Mesaj yükü; 7.x'te `ReadOnlyMemory<byte>` ve callback sonrası belleğin güvenliğini düşünmelisin.
- `ea.DeliveryTag`: Bu channel'daki teslimi tanımlayan numara.
- `multiple:false`: Yalnızca belirtilen teslimi ACK et.
- `autoAck:false`: İş tamamlanınca manuel onay.
- `consumerTag`: Aboneliği tanımlayan kimlik; `BasicCancelAsync` ile iptal edilebilir.

### 15.6 Üretim koduna geçerken ayrıca ele alacağız

- Long-lived connection / channel sahipliği ve disposal.
- Channel thread-safety ve publish serileştirme.
- Publisher confirms ve basic.return.
- `BackgroundService` üzerinde consumer yaşam döngüsü.
- Manual ACK/NACK ve cancellation.
- Retry / DLQ / idempotency.
- OpenTelemetry, logs/metrics.
- Integration tests ve Docker test ortamı.

---

## 16. Management UI ile 6 laboratuvar

> Her laboratuvarda `learning` vhost'unu kullan. RabbitMQ Management UI menüleri sürüme göre biraz farklı adlandırılmış olabilir. Kod yazmadan exchange/queue davranışını gözlemlemek amacımız.

### Lab 1 — Direct exchange ile tam eşleşme

1. **Exchanges** → `demo.direct` adında **direct**, **durable** bir exchange oluştur.
2. **Queues and Streams** → `demo.error.queue` ve `demo.info.queue` adlı **durable classic** queue'ları oluştur.
3. `demo.error.queue` → Bindings → `demo.direct` ile `error` routing key üzerinden bağla.
4. `demo.info.queue` → Bindings → `demo.direct` ile `info` routing key üzerinden bağla.
5. `demo.direct` → Publish alanında routing key `error`, payload `First error` gönder.
6. Queue listesine dön: `demo.error.queue` Ready +1; `demo.info.queue` değişmemeli.
7. Aynı exchange'e `warning` routing key ile publish yap. **Eşleşme yoksa** mesajın sonuçsuz kalabileceğini gözlemle.

**Beklenen öğrenme:** Direct **tam eşleşir**; `warning` için `error` veya `info` binding'i çalışmaz.

### Lab 2 — Fanout ile gerçek broadcast

1. `demo.fanout` adlı fanout exchange yarat.
2. `demo.email.queue` ve `demo.audit.queue` adlı durable queue'lar oluştur.
3. İki queue'yu `demo.fanout` exchange'ine bind et; routing key'in değeri önemli değil.
4. Tek payload gönder: `User registered`.
5. İki queue'da da Ready artmalı.
6. Bir queue'dan mesaj tüketirsen diğer queue'daki kopyanın durumunun bağımsız olduğunu gör.

**Beklenen öğrenme:** *Bir exchange, iki queue = iki kopya.*

### Lab 3 — Topic wildcard'larını deneyerek öğren

1. `demo.topic` adlı topic exchange oluştur.
2. `demo.eu.queue` queue'sunu `order.*.eu` ile bağla.
3. `demo.allorders.queue` queue'sunu `order.#` ile bağla.
4. Aşağıdaki key'lerle ayrı ayrı mesaj gönder:

| Routing key | `demo.eu.queue` | `demo.allorders.queue` |
|---|---:|---:|
| `order.created.eu` | ✅ | ✅ |
| `order.cancelled.tr` | ❌ | ✅ |
| `order` | ❌ | ✅ |
| `payment.created.eu` | ❌ | ❌ |

**Beklenen öğrenme:** `*` = tam bir segment, `#` = sıfır veya çok segment.

### Lab 4 — Mesaj consumer yokken bekliyor mu?

1. Durable `demo.waiting.queue` oluştur.
2. Default exchange (`""`) üzerinden `routingKey = demo.waiting.queue` ile mesaj publish et (UI'da default exchange seçimi/adlandırması sürüme göre değişebilir).
3. Henüz consumer yokken **Ready=1** gör.
4. RabbitMQ container'ını **restart** et; volume'u silme.
5. **Persistent** yayın ayarıyla oluşturulmuş mesajın uygun koşullarda queue'da kalmasını; **transient** mesajın ise recovery sırasında kaybolabileceğini karşılaştır.

**Önemli:** UI'nın publish formunda delivery mode ayarının nerede olduğu sürüme göre değişebilir. Testten önce persistent mode'u açıkça doğrula.

### Lab 5 — Management UI'dan mesaj alma ve silme

1. `demo.direct` üzerinden eşleşen queue'ya 2 mesaj gönder.
2. Queue ayrıntısında **Get messages** bölümünü bul.
3. `Requeue` içeren seçenekle mesajı alırsan mesajı tekrar Ready görebilirsin.
4. **Ack / no requeue** gibi bir seçenekle alırsan ilgili mesaj queue'dan kaldırılabilir.

⚠️ UI'dan mesaj çekmek **yalnızca gözlemleme** değildir; seçtiğin ACK/requeue davranışına göre **mesajı tüketebilir**. Kritik queue'da rastgele kullanma.

### Lab 6 — Durability ve Docker volume

1. `demo.persistence.queue` adında durable classic queue yarat.
2. Persistent mesaj yolla ve broker restart sonrası durumuna bak.
3. `docker compose down` ve ardından `docker compose up -d` uygula; **volume duruyorsa** mesaj/topoloji durumu korunmalıdır.
4. Sadece eğitim verileri için istersen `docker compose down -v` uygula; tekrar başladığında eski queue'ların ve mesajların gitmiş olacağını gör.

**Beklenen öğrenme:** *Broker restart* ile *volume'u silmek* aynı şey değildir.

---

## 17. Operasyon, güvenlik ve sorun giderme

### 17.1 Yararlı komutlar

```powershell
docker compose ps
docker compose logs --tail 100 rabbitmq
docker exec rabbitmq-local rabbitmq-diagnostics -q ping
docker exec rabbitmq-local rabbitmqctl list_vhosts
docker exec rabbitmq-local rabbitmqctl list_users
docker exec rabbitmq-local rabbitmqctl list_exchanges -p learning name type durable
docker exec rabbitmq-local rabbitmqctl list_queues -p learning name durable messages_ready messages_unacknowledged consumers
```

`list_exchanges` / `list_queues` belirttiğin vhost içinde görüntüler. Daha fazlası için `rabbitmqctl help` veya Management UI kullanılabilir.

### 17.2 Sık görülen hatalar

| Belirti / hata | Muhtemel neden | Kontrol |
|---|---|---|
| `Connection refused` / bağlantı kurulamıyor | Container kapalı, port uyuşmuyor, Docker engine çalışmıyor. | `docker compose ps`, `docker logs`, `localhost:5672`. |
| Management UI açılmıyor | Yanlış port, image management değil, container hazır değil. | `rabbitmq:4.3.6-management`, `15672`, logs. |
| `ACCESS_REFUSED` | Kullanıcı/parola/vhost/permission yanlış; varsayılan `guest` uzaktan kısıtlıdır. | `.env` ve vhost izinleri; user'ı Yönetim UI'dan kontrol et. |
| `.env` şifresini değiştirdim ama giriş olmadı | İlk kurulmuş broker kullanıcı/parolası persisted volume'da saklanır. | Broker içinde kullanıcı parolasını güncelle veya **eğitim verisi silinecekse** `down -v`. |
| `PRECONDITION_FAILED inequivalent arg` | Var olan queue/exchange farklı durable/type/args ile tekrar declare edilmeye çalışılıyor. | Var olan tanımı kontrol et; veri silmeden yeniden oluşturma yapma. |
| `NOT_FOUND - no exchange/queue` | Ad veya vhost yanlış, nesne oluşturulmamış. | UI'da doğru vhost ve nesneyi kontrol et; channel kapanmış olabilir. |
| `RESOURCE_LOCKED` | Exclusive queue başka connection'a ait. | Queue sahibini ve connection yaşam döngüsünü incele. |
| Publish yaptım, queue artmadı | Yanlış exchange/routing key/binding, yanlış vhost, consumer hemen tüketti veya unroutable mesaj. | Binding'ler, Ready/Unacked, consumer, returned message, AE. |
| Ready artıyor ama işlenmiyor | Consumer yok/çalışmıyor, prefetch ya da işleme sorunu. | Consumer count, Unacked, worker logs. |
| Unacked sürekli yüksek | Consumer yavaş/takılmış veya ACK unutulmuş. | Prefetch, hata yakalama, ACK/NACK yönetimi. |
| Disk/memory alarmı | Broker kaynak limiti zorlanıyor. | Overview/node metrics, disk, limit ve backpressure politikaları. |

### 17.3 Güvenlik kontrol listesi

- Yerel demo kullanıcı bilgilerini **production'da kullanma**.
- Production'da kullanıcıları rol/izin bazında ayır; AMQP ve Management UI'ı internete sınırsız açma.
- TLS (AMQPS), kimlik doğrulama, network ACL/firewall, secrets yönetimi planla.
- `RABBITMQ_DEFAULT_PASS` gibi değerlerin Docker ortam metadata'sında görünebileceğini unutma; production'da secret yönetimini ayrıca ele al.
- `.env` dosyasını Git'e ekleme; yanlışlıkla gönderilen sırları rotate et.
- Message payload içine gereksiz kişisel/hassas veri koyma; loglarda payload maskeleme düşün.
- Topoloji değişikliklerinde queue'yu silerek yeniden yaratmanın veri kaybına yol açabileceğini unutma.

### 17.4 Production'a geçmeden önce

**Şu anki compose tek RabbitMQ node'u içeren bir eğitim ortamıdır.** HA cluster, disk kapasite planı, monitoring, yedekleme/restore, alarms, quotas, publisher confirms, outbox/inbox, deployment güncellemeleri ve TLS içermez. **Tek node + tek volume = tek hata alanı.**

---

## 18. Sık sorular, sözlük, öğrenme planı

### Sık sorulan sorular

**“Mesaj gönderildiğinde exchange'de mi tutuluyor?”**  
AMQP 0-9-1'in normal exchange/queue modelinde exchange mesajı route eder; bekleme/depolama queue tarafındadır.

**“Queue'ya hiç consumer bağlanmazsa ne olur?”**  
Var olan queue'ya doğru route edilmiş mesaj Ready durumda bekleyebilir; queue tipi, TTL, length limitleri ve storage koşulları sonucu etkiler.

**“Consumer çalıştıktan sonra mesaj hep duruyor mu?”**  
Normal queue'da başarılı ACK sonrası kaldırılır. Kalıcı arşiv/replay için Streams gibi başka modeller düşün.

**“Durable queue yaratınca persistent mesaj özelliği de otomatik açılır mı?”**  
Hayır, classic AMQP publish için ayrı message delivery mode vardır. Quorum queue kendi kalıcılık kurallarıyla çalışır.

**“Fanout, tüm consumer'lara mı mesaj yollar?”**  
Fanout **bağlı queue'lara** route eder. Aynı queue'nun birden çok consumer'ı iş paylaşımı yapar.

**“ACK vermezsem mesaj silinir mi?”**  
Manual ACK modunda normal durumda Unacked kalır; connection/channel kaybında tekrar teslim edilebilir. TTL/recovery ve başka koşullar ayrıca etkilidir.

**“Producer RabbitMQ'ya mesajı bastı, HTTP request başarılı döndü; iş garantili mi?”**  
Hayır. Publisher confirm, doğru binding, mesaj/queue durability ve consumer'ın iş sonucu ayrı konulardır.

**“RabbitMQ Kafka gibi geçmiş mesajları tekrar okutur mu?”**  
Normal classic/quorum queue'lar için ACK sonrası hayır. RabbitMQ Streams replay sağlar; Kafka da log/offset tabanlı yaklaşımıyla farklıdır.

### Sözlük

| Terim | Tek cümlelik anlam |
|---|---|
| Producer / Publisher | Mesajı üreten ve yayınlayan uygulama. |
| Broker | Mesajlaşma sunucusu. |
| Exchange | Mesajı hedeflere yönlendiren routing bileşeni. |
| Queue | Consumer'ın işleyeceği mesajların bulunduğu yapı. |
| Binding | Exchange → queue (veya başka exchange) yönlendirme bağlantısı. |
| Routing key | Publisher'ın routing amacıyla verdiği anahtar. |
| Vhost | RabbitMQ mantıksal izolasyon alanı. |
| Connection | TCP/AMQP bağlantısı. |
| Channel | Connection içinde mantıksal AMQP oturumu. |
| ACK | Mesaj işleme başarısının tüketici tarafından onaylanması. |
| NACK / Reject | Mesajın reddedilmesi/yeniden sıralama tercihi. |
| Prefetch | Consumer'a teslim edilip henüz ACK edilmemiş mesaj sınırı. |
| DLX / DLQ | Başarısız/expire mesajları yeniden route eden exchange / hedef queue. |
| TTL | Süre sınırı. |
| Publisher confirm | Broker'ın publish için sorumluluk aldığını bildirmesi. |
| Idempotency | Aynı mesaj tekrar gelse bile iş etkisinin tekrarlanmamasını sağlama. |
| Quorum queue | Dayanıklı, replicated, Raft tabanlı queue tipi. |
| Stream | Tekrar okunabilir append-only mesaj log'u. |

### Sonraki kodlama aşamaları

Bu README'yi tamamladığımızda sırayla:

1. `RabbitMQ.Producer` (.NET console): Connection, channel, durable exchange/queue, binding.
2. `RabbitMQ.Consumer` (`BackgroundService`): `AsyncEventingBasicConsumer`, manual ACK, prefetch.
3. Direct/Fanout/Topic/Headers için ayrı, çalışır örnekler ve testler.
4. Persistent publish, publisher confirms, mandatory return.
5. Retry/DLX/DLQ, poison message, idempotency.
6. ASP.NET Core API + Worker Service + Docker Compose entegrasyonu.
7. Gözlemlenebilirlik, concurrency, integration tests, ileri production senaryoları.

---

## 19. Resmî kaynaklar

Versiyon değişebileceği için kritik ayrıntıları düzenli olarak aşağıdaki **resmî** kaynaklardan doğrula:

- [RabbitMQ — Installing RabbitMQ](https://www.rabbitmq.com/docs/download)
- [RabbitMQ — Exchanges](https://www.rabbitmq.com/docs/exchanges)
- [RabbitMQ — Queues (RabbitMQ 4.3 davranışları)](https://www.rabbitmq.com/docs/queues)
- [RabbitMQ — Consumer acknowledgements and publisher confirms](https://www.rabbitmq.com/docs/confirms)
- [RabbitMQ — Reliability guide](https://www.rabbitmq.com/docs/reliability)
- [RabbitMQ — Publishers](https://www.rabbitmq.com/docs/publishers)
- [RabbitMQ — Consumer Prefetch](https://www.rabbitmq.com/docs/consumer-prefetch)
- [RabbitMQ — Dead Letter Exchanges](https://www.rabbitmq.com/docs/dlx)
- [RabbitMQ — Time-To-Live and Expiration](https://www.rabbitmq.com/docs/ttl)
- [RabbitMQ — Streams](https://www.rabbitmq.com/docs/streams)
- [RabbitMQ — Management Plugin](https://www.rabbitmq.com/docs/management)
- [RabbitMQ — .NET Client API Guide (7.x)](https://www.rabbitmq.com/client-libraries/dotnet-api-guide)
- [NuGet — RabbitMQ.Client](https://www.nuget.org/packages/RabbitMQ.Client)
- [Docker Official RabbitMQ Image Tags](https://hub.docker.com/_/rabbitmq/tags)

---

**Bu repodaki amaç yalnızca mesaj göndermek değil; mesajın hangi koşulda yönlendirildiğini, saklandığını, yeniden teslim edildiğini ve kaybolabileceğini açıklayabilecek düzeyde RabbitMQ öğrenmek.**
