create table `server_events1` (
  `id`         int not null auto_increment,
  `type`       int not null,
  `timestamp`  datetime not null,
  `foreign_id` varchar(255) not null,
  primary key (`id`)
);

create table `server_events2` (
  `id`         int not null auto_increment,
  `type`       int not null,
  `timestamp`  datetime not null,
  `foreign_id` varchar(255) not null,
  primary key (`id`)
);

create table `server_cursors` (
  `id`            varchar(255) not null,
  `updated_at`    datetime not null,
  `last_event_id` int not null,
  primary key (`id`)
);