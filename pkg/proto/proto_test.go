package proto

import(
  "bufio"
  "reflect"
  "bytes"
  "fmt"
  "testing"
)

/* Assertions */
func assertEqual(t *testing.T, expected, result interface{}, message string) {
  if ! reflect.DeepEqual(expected, result) {
    t.Fatalf("expected <%#v> but got <%#v> (%s)", expected, result, message)
  }
}

/* Helpers */
func setupPipe(chunks ... string) (buf *bufio.Reader, ch chan *Request) {
  ch = make(chan *Request)
  data := new(bytes.Buffer);
  buf = bufio.NewReader(data);
  for _, chunk := range(chunks) {
    data.WriteString(chunk)
  }
  return
}

func TestOneRequestOnePart(t *testing.T) {
  buf, ch := setupPipe("*1\r\n$3\r\nfoo\r\n")
  go Scan(buf, ch);

  var req *Request
  req = <- ch

  assertEqual(t, 1, len(req.Parts), "")
  assertEqual(t, "foo", string(req.Parts[0]), "")
  if req.Err != nil { t.Fatalf("expected %#v to be nil", req.Err) }
}

func TestTwoRequestsOnePart(t *testing.T) {
  buf, ch := setupPipe(
    "*1\r\n$3\r\nfoo\r\n",
    "*1\r\n$3\r\nbar\r\n",
  )
  go Scan(buf, ch);

  var req *Request
  req = <- ch

  assertEqual(t, 1, len(req.Parts), "")
  assertEqual(t, "foo", string(req.Parts[0]), "")
  if req.Err != nil {
    t.Fatalf("expected %#v to be nil", req.Err)
  }

  req = <- ch

  assertEqual(t, 1, len(req.Parts), "")
  assertEqual(t, "bar", string(req.Parts[0]), "")
  if req.Err != nil { t.Fatalf("expected %#v to be nil", req.Err) }
}

func TestOneRequestTwoParts(t *testing.T) {
  buf, ch := setupPipe("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
  go Scan(buf, ch);

  var req *Request
  req = <- ch

  assertEqual(t, 2, len(req.Parts), "")
  assertEqual(t, "foo", string(req.Parts[0]), "")
  assertEqual(t, "bar", string(req.Parts[1]), "")
  if req.Err != nil { t.Fatalf("expected %#v to be nil", req.Err) }
}

func TestScanNumber(t *testing.T) {

}

func TestOneRequestWithError(t *testing.T) {
  testCases := []string{
    "a",
    "$a",
    "$1a",
    "$1",
    "*a",
    "*1a",
    "*1\na",
    "*1\n$a",
    "*1\n$1a",
    "a\n",
    "$a\n",
    "$1a\n",
    "$1\n",
    "*a\n",
    "*1a\n",
    "*1\na\n",
    "*1\n$a\n",
    "*1\n$1a\n",
  }

  for _, data := range(testCases) {
    buf, ch := setupPipe(data)
    go Scan(buf, ch);

    var req *Request
    req = <- ch

    assertEqual(t, ProtocolError, req.Err, fmt.Sprintf("%q", data))
    if req.Parts != nil { t.Fatalf("expected %#v to be nil", req.Err) }
  }
}

func TestScanNumberWithError(t *testing.T) {
  testCases := []string{
    "a\n",
    "$a\n",
    "$1a\n",
    "a",
    "$a",
    "$1a",
  }

  for _, data := range(testCases) {
    buf, _ := setupPipe(data)
    n, err := scanNumber(buf, '$')

    assertEqual(t, uint64(0), n, fmt.Sprintf("%q", data))
    if err == nil {
        t.Errorf("expected error for %q, got nil", data)
    }
  }
}
