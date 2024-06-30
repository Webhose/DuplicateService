import argparse
import redis

# Initialize Redis connection
r = redis.Redis(host='tbcrawler21', port=6379, db=0)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--is_syndicate_on', choices=['True', 'False'], default='False', type=str)
    args = parser.parse_args()
    return args


def main():
    arguments = get_args()
    value = arguments.is_syndicate_on
    r.set('is_syndicate_on', value)
    print('is_syndicate_on flag is set to: {}'.format(arguments.is_syndicate_on))


if __name__ == '__main__':
    main()
