select subquery.name, cp.nickname from (select Trainer.id as id, Trainer.name as name, max(CatchedPokemon.level) as mx from Trainer join CatchedPokemon on Trainer.id = CatchedPokemon.owner_id group by Trainer.id having count(CatchedPokemon.id) >= 4) as subquery join CatchedPokemon as cp on cp.owner_id = subquery.id where cp.level = mx order by cp.nickname;
