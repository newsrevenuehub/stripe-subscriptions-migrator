build:
	docker-compose build stripe-subscriptions-migrator
	docker-compose run stripe-subscriptions-migrator


build-with-cnb:
	pack build stripe-subscriptions-migrator --builder heroku/buildpacks
