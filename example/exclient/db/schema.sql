create table `client_cursors` (
  `id`            varchar(255) not null,
  `updated_at`    datetime not null,
  `last_event_id` int not null,
  primary key (`id`)
);