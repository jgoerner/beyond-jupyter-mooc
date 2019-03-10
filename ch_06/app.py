from apistar import App, Route

def welcome(name=None):
    if name:
        return {"msg": "Welcome to API Star, %s" % name}
    else:
        return {"msg": "Welcome to API Star!"}

routes = [Route("/", method="GET", handler=welcome)]

app = App(routes=routes)

if __name__ == "__main__":
    app.serve("127.0.0.1", 5000, debug=True)
