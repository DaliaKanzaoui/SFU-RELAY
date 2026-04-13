import { Routes } from '@angular/router';
import {VideoComponent} from './video/video';
import {DashcamServerComponent} from './dashcam-server/dashcam-server';

export const routes: Routes = [
  { path: '', component: VideoComponent },
  { path: 'client', component: VideoComponent },
  { path: 'server', component: DashcamServerComponent }
];
