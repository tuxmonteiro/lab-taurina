package tuxmonteiro.lab.taurina.enumerator;

public enum Proto {

        HTTPS_1(true),
        HTTPS_2(true),
        HTTP_1(false),
        HTTP_2(false);

        private final boolean ssl;

        Proto(boolean ssl) {
            this.ssl = ssl;
        }

        public static Proto schemaToProto(String schema) {
            switch (schema) {
                case "h2":
                    return HTTP_2;
                case "h2c":
                    return HTTPS_2;
                case "http":
                    return HTTP_1;
                case "https":
                    return HTTPS_1;
            }
            return Proto.valueOf(schema);
        }
    }

