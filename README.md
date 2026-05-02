# tp-kafka-java

Projet Java 17 de demonstration Kafka pour simuler plusieurs flux d'evenements dans un contexte de vente et de point de caisse.

## Presentation

Le projet contient plusieurs producteurs et consommateurs Kafka autour de deux cas principaux :

- un flux de ventes simple sur le topic `ventes`
- un flux de commandes structurees sur le topic `commandes`
- un flux d'evenements de caisse sur le topic `pos-events`
- un flux d'alertes generees automatiquement sur le topic `alertes-retours`

Les messages sont serialises en JSON avec Jackson et traites via les clients Apache Kafka.

## Technologies utilisees

- Java 17
- Apache Kafka clients 3.9.0
- Jackson Databind pour le JSON
- SLF4J Simple pour les logs
- Maven pour la compilation et l'execution

## Structure du projet

- `src/main/java/tn/utm/kafka/SimpleProducer.java` : envoie des messages de vente sur `ventes`
- `src/main/java/tn/utm/kafka/SimpleConsumer.java` : consomme et affiche les messages de `ventes`
- `src/main/java/tn/utm/kafka/CommandeProducer.java` : publie des commandes JSON sur `commandes`
- `src/main/java/tn/utm/kafka/CommandeConsumer.java` : lit les commandes et affiche leur contenu
- `src/main/java/tn/utm/kafka/SimulateurCaisse.java` : genere des evenements de caisse aleatoires sur `pos-events`
- `src/main/java/tn/utm/kafka/ChiffreAffairesParVille.java` : calcule le chiffre d'affaires par ville a partir de `pos-events`
- `src/main/java/tn/utm/kafka/DetecteurAnomalies.java` : detecte certains retours suspects et publie des alertes sur `alertes-retours`
- `src/main/java/tn/utm/kafka/Commande.java` : modele de commande
- `src/main/java/tn/utm/kafka/EvenementCaisse.java` : modele d'evenement de caisse

## Pre-requis

- Java 17 ou plus
- Maven 3.8+ 
- Un broker Kafka accessible sur `localhost:9092`

## Compilation

```bash
mvn clean package
```

## Execution des exemples

Lancez d'abord Kafka, puis ouvrez un terminal par composant si vous souhaitez tester les flux en parallele.

Exemples :

```bash
mvn exec:java -Dexec.mainClass=tn.utm.kafka.SimpleProducer
mvn exec:java -Dexec.mainClass=tn.utm.kafka.SimpleConsumer
```

```bash
mvn exec:java -Dexec.mainClass=tn.utm.kafka.CommandeProducer
mvn exec:java -Dexec.mainClass=tn.utm.kafka.CommandeConsumer
```

```bash
mvn exec:java -Dexec.mainClass=tn.utm.kafka.SimulateurCaisse
mvn exec:java -Dexec.mainClass=tn.utm.kafka.ChiffreAffairesParVille
mvn exec:java -Dexec.mainClass=tn.utm.kafka.DetecteurAnomalies
```

## Remarques

- Les consommateurs utilisent un commit manuel des offsets.
- Certains traitements tournent en boucle infinie jusqu'a interruption manuelle.
- Le projet sert de base pedagogique pour illustrer les producteurs, consommateurs et traitements temps reel avec Kafka.