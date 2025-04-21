def get-token():

    updateNeeded = True
    userUID = int(os.getuid())

    # Select token path
    bearerTokenFile = os.getenv('BEARER_TOKEN_FILE')

    if not bearerTokenFile:
        if os.path.isdir('/run/user/%d' % userUID):
        bearerTokenFile = '/run/user/%d/bt_u%d' % (userUID, userUID)
        else:
        bearerTokenFile = '/tmp/bt_u%d' % userUID

    if jsonDict['verbose']:
        print('Bearer token file %s' % bearerTokenFile, file=sys.stderr)

    try:
        bearerTokenFileMode = os.stat(bearerTokenFile).st_mode
    except:
        if jsonDict['verbose']:
        print('No bearer token file to check permissions of', file=sys.stderr)
    else:
        if not (bearerTokenFileMode & stat.S_IWUSR):
        print('Token file %s is not writeable - exiting' % bearerTokenFile, 
                file=sys.stderr)
        sys.exit(1)
    
    try:
        bearerToken = open(bearerTokenFile, 'r').read()
        bearerTokenDict = json.loads(base64.urlsafe_b64decode(
                                        bearerToken.split('.')[1] + '=='))  
        bearerTokenExpires = int(bearerTokenDict['exp'])
    except:
        updateNeeded = True
        if jsonDict['verbose']:
        print('No valid bearer token file to check', file=sys.stderr)
    else:
        if jsonDict['verbose']:
        print('Bearer token expires in %d seconds' %  
                (bearerTokenExpires - int(time.time())),
                file=sys.stderr)
        
        if bearerTokenExpires > int(time.time()) + 3600:
        if jsonDict['verbose']:
            updateNeeded = False
            print('Bearer token not near expiry', file=sys.stderr)

    # Select proxy path
    x509ProxyFile = os.getenv('X509_USER_PROXY')

    if not x509ProxyFile:
        x509ProxyFile = '/tmp/x509up_u%d' % userUID
    
    if jsonDict['verbose']:
        print('X509 proxy file %s' % x509ProxyFile, file=sys.stderr)

    try:
        x509ProxyFileMode = os.stat(x509ProxyFile).st_mode
    except:
        updateNeeded = True
        if jsonDict['verbose']:
        print('No proxy file - update needed', file=sys.stderr)      
    else:
        if not (x509ProxyFileMode & stat.S_IWUSR):
        print('Proxy file %s is not writeable - exiting' % x509ProxyFile, 
                file=sys.stderr)
        sys.exit(1)

        if os.stat(x509ProxyFile).st_mtime < int(time.time()) - 86400:
        # If proxy older than 24 hours then force an update
        updateNeeded = True 
        if jsonDict['verbose']:
            print('Proxy is over 24 hours old - updated required', file=sys.stderr)
    
    if not updateNeeded:
        print('Updated not needed - exiting', file=sys.stderr)
        sys.exit(0)
    
    try:
        tempDir = tempfile.TemporaryDirectory()
        tempDirName = tempDir.name
    # For testing. mkdtemp() leaves the temporary directory in place afterwards
    #    tempDirName = tempfile.mkdtemp()

        # Minimal openssl.cnf file to make  openssl req  happy
        open('%s/openssl.cnf' % tempDirName, 'w').write(
            '[req]\ndistinguished_name=dn\nattributes=attr\n[dn]\n[attr]\n')

        os.system('openssl req -batch -nodes -newkey rsa:2048 '
                '-keyout %s/key.pem -out %s/csr.pem -subj "/CN=123" '
                '-config %s/openssl.cnf %s' 
                % (tempDirName, tempDirName, tempDirName, 
                    '' if jsonDict['verbose'] else '2>/dev/null'))

        with open('%s/csr.pem' % tempDirName, 'r') as f:
        jsonDict['proxy_csr'] = f.read()

        with open('%s/key.pem' % tempDirName, 'r') as f:
        proxyKey = f.read()
    except Exception as e:
        print('Failed to create X.509 proxy request with openssl command (%s)' 
            % str(e), file=sys.stderr)
        sys.exit(1)
