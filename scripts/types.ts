import { convertFromDirectory } from 'joi-to-typescript';

// noinspection JSIgnoredPromiseFromCall
convertFromDirectory({
  schemaDirectory: './src/config',
  typeOutputDirectory: './src/interfaces',
  debug: true,
});
