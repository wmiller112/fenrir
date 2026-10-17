package moonlight

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	v1alpha1types "games-on-whales.github.io/direwolf/pkg/api/v1alpha1"
	"games-on-whales.github.io/direwolf/pkg/generic"
	"games-on-whales.github.io/direwolf/pkg/util"
)

func TestAuthenticatedMiddleware(t *testing.T) {
	paired := &x509.Certificate{Raw: []byte("paired client")}
	unknown := &x509.Certificate{Raw: []byte("client whose pairing is gone")}

	pairings := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	users := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	_ = pairings.Add(&v1alpha1types.Pairing{
		ObjectMeta: metav1.ObjectMeta{Name: hex.EncodeToString(util.Hash(paired.Raw)), Namespace: "direwolf"},
		Spec:       v1alpha1types.PairingSpec{UserReference: v1alpha1types.UserReference{Name: "alex"}},
	})
	_ = users.Add(&v1alpha1types.User{ObjectMeta: metav1.ObjectMeta{Name: "alex", Namespace: "direwolf"}})

	s := &RESTServer{
		PairingsLister: generic.NewLister[*v1alpha1types.Pairing](pairings).Namespaced("direwolf"),
		UserLister:     generic.NewLister[*v1alpha1types.User](users).Namespaced("direwolf"),
	}

	var gotUser any
	handler := s.authenticatedMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser = r.Context().Value(userContextKey{})
		w.WriteHeader(http.StatusOK)
	}))
	request := func(path string, cert *x509.Certificate) int {
		gotUser = nil
		r := httptest.NewRequest(http.MethodGet, "https://direwolf"+path, http.NoBody)
		r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}

	if code := request("/applist", paired); code != http.StatusOK || gotUser == nil {
		t.Errorf("paired /applist: status %d, user %v", code, gotUser)
	}
	// An unknown client still learns it isn't paired, so it can pair again
	if code := request("/serverinfo", unknown); code != http.StatusOK || gotUser != nil {
		t.Errorf("unknown /serverinfo: status %d, user %v, want 200 without a user", code, gotUser)
	}
	// but gets nothing else
	for _, path := range []string{"/applist", "/launch"} {
		if code := request(path, unknown); code != http.StatusUnauthorized {
			t.Errorf("unknown %s: status %d, want 401", path, code)
		}
	}
}
