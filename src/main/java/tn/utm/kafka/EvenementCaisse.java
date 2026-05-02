package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.UUID;

public class EvenementCaisse {

    private static final ObjectMapper mapper = new ObjectMapper();

    private String type;
    private String idCaisse;
    private String ville;
    private String timestamp;
    private double montant;

    public EvenementCaisse(String ville, String type, double montant) {
        this.ville     = ville;
        this.type      = type;
        this.montant   = montant;
        this.idCaisse  = "CAISSE-" + ville.toUpperCase() + "-" + (int)(Math.random() * 10 + 1);
        this.timestamp = Instant.now().toString();
    }

    public String getVille()   { return ville;   }
    public String getType()    { return type;    }
    public double getMontant() { return montant; }

    // Sérialisation en JSON
    public String toJson() {
        try {
            ObjectNode node = mapper.createObjectNode();
            node.put("type",      type);
            node.put("idCaisse",  idCaisse);
            node.put("ville",     ville);
            node.put("timestamp", timestamp);
            node.put("montant",   montant);
            return mapper.writeValueAsString(node);
        } catch (Exception e) {
            throw new RuntimeException("Erreur sérialisation JSON", e);
        }
    }

    @Override
    public String toString() {
        return toJson();
    }
}