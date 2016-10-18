<?php

namespace Drupal\workspace\Controller;

use Drupal\Core\Controller\ControllerBase;
use Drupal\Core\Url;
use Drupal\workspace\Entity\Workspace;
use Drupal\workspace\Entity\WorkspaceType;
use Drupal\workspace\Entity\WorkspaceTypeInterface;

class WorkspaceController extends ControllerBase {

  public function add() {
    $types = WorkspaceType::loadMultiple();
    if ($types && count($types) == 1) {
      $type = reset($types);
      return $this->addForm($type);
    }
    if (count($types) === 0) {
      return array(
        '#markup' => $this->t('You have not created any Workspace types yet. Go to the <a href=":url">Workspace type creation page</a> to add a new Workspace type.', [
          ':url' => Url::fromRoute('entity.workspace_type.add_form')->toString(),
        ]),
      );
    }

    return array('#theme' => 'workspace_add_list', '#content' => $types);

  }

  public function addForm(WorkspaceTypeInterface $workspace_type) {
    $workspace = Workspace::create([
      'type' => $workspace_type->id()
    ]);
    return $this->entityFormBuilder()->getForm($workspace);
  }

  public function getAddFormTitle(WorkspaceTypeInterface $workspace_type) {
    return $this->t('Add %type workspace', array('%type' => $workspace_type->label()));
  }
}
