package ClickStreamFeeder

import (
    "fmt"
    "errors"
    "strings"
    "strconv"
    "net/url"
    "net/http"
    "appengine"
    "appengine/urlfetch"
)

func init() {
    http.HandleFunc("/", handler)
}

func handler(w http.ResponseWriter, r *http.Request) {
    qValues, err := url.ParseQuery(r.URL.RawQuery)
    if err != nil {
        fmt.Fprint(w, "did not send properly formatted query string: " + err.Error())
        w.WriteHeader(http.StatusBadRequest)
    } else {
        err = ValidateQueryString(qValues)
        if err != nil {
            fmt.Fprint(w, "did not send all required paramters:  " + err.Error())
            w.WriteHeader(http.StatusBadRequest)
        } else {
            err = SendClickstreams(qValues, r, w)
            if err != nil {
                fmt.Fprint(w, "did not post data: " + err.Error() + "\n")
                w.WriteHeader(http.StatusBadRequest)
            } else {
                fmt.Fprint(w, "Request received and processed.  Data should show up momentarily\n")
            }
        }
    }
}

//  parameter validation
func ValidateQueryString(queryParams url.Values) (error) {
    _, targetOK := queryParams["targetURI"]
    _, countOK := queryParams["count"]
    _, prefixOK := queryParams["prefix"]

    if !countOK {
        return errors.New("Did not include count\n")
    }
    if !targetOK {
        return errors.New("Did not include targetURI\n")
    }
    if !prefixOK {
        return errors.New("Did not include prefix\n")
    }

    return nil
}


func SendClickstreams(qValues url.Values, r *http.Request, w http.ResponseWriter) (error) {
    countStr := qValues.Get("count")
    count, _ := strconv.Atoi(countStr)
    targetURI := qValues.Get("targetURI")
    prefix := qValues.Get("prefix")

    var err error
    for i := 0; i < count; i++ {
        iStr := strconv.Itoa(i)
        req, _ := http.NewRequest("POST", targetURI, strings.NewReader("{\"body\":\""+ prefix + "-" + iStr + "\"}"))
        req.Header.Set("Content-Type", "application/json")

        c := appengine.NewContext(r)
        client := urlfetch.Client(c)

        _, err = client.Do(req)
        if err != nil {
            break
        }
    }
    return err
}
