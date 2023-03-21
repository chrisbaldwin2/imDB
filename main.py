import frontend 
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    parser.add_argument('--port', type=int, default=5001, help='Port to run on')
    args = parser.parse_args()
    # start the flask app
    frontend.app.run(debug=args.debug, port=args.port)
    
if __name__ == '__main__':
    main()