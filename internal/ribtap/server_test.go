package ribtap

import "testing"

func TestConfigValidate(t *testing.T) {
	good := Config{ASN: 65010, RouterID: "10.0.0.1", ListenPort: 1790}
	if err := good.validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	bad := map[string]Config{
		"no asn":          {RouterID: "10.0.0.1", ListenPort: 1790},
		"bad router id":   {ASN: 65010, RouterID: "not-an-ip", ListenPort: 1790},
		"empty router id": {ASN: 65010, ListenPort: 1790},
		"zero port":       {ASN: 65010, RouterID: "10.0.0.1"},
		"disabled port":   {ASN: 65010, RouterID: "10.0.0.1", ListenPort: -1},
	}
	for name, c := range bad {
		if err := c.validate(); err == nil {
			t.Errorf("invalid config %q accepted", name)
		}
	}
}

func TestNewServerValidates(t *testing.T) {
	if _, err := NewServer(Config{}, nil); err == nil {
		t.Fatal("NewServer should reject an empty config")
	}
	s, err := NewServer(Config{ASN: 65010, RouterID: "10.0.0.1", ListenPort: 1790}, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if s.BGP() == nil {
		t.Error("BGP() should expose the embedded server")
	}
}
